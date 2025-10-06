package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pasarguard/node/config"
	"github.com/pasarguard/node/controller"
	"github.com/pasarguard/node/controller/rest"
	"github.com/pasarguard/node/controller/rpc"
	natslistener "github.com/pasarguard/node/nats"
	"github.com/pasarguard/node/tools"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	addr := fmt.Sprintf("%s:%d", cfg.NodeHost, cfg.ServicePort)

	tlsConfig, err := tools.LoadTLSCredentials(cfg.SslCertFile, cfg.SslKeyFile)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Starting Node: v%s", controller.NodeVersion)

	var shutdownFunc func(ctx context.Context) error
	var service controller.Service

	if cfg.ServiceProtocol == "rest" {
		shutdownFunc, service, err = rest.StartHttpListener(tlsConfig, addr, cfg)
	} else {
		shutdownFunc, service, err = rpc.StartGRPCListener(tlsConfig, addr, cfg)
	}

	if err != nil {
		log.Fatal(err)
	}

	defer service.Disconnect()

	// Initialize NATS listener if enabled
	var natsListener *natslistener.NATSListener
	if cfg.NATSEnabled {
		// Get the controller from service
		var ctrl *controller.Controller
		
		// Try rest service type
		if restService, ok := service.(*rest.Service); ok {
			ctrl = &restService.Controller
		}
		// Try rpc service type  
		if rpcService, ok := service.(*rpc.Service); ok {
			ctrl = &rpcService.Controller
		}

		if ctrl != nil {
			natsListener, err = natslistener.New(cfg, ctrl)
			if err != nil {
				log.Printf("Warning: Failed to create NATS listener: %v", err)
			} else if natsListener != nil {
				if err := natsListener.Connect(); err != nil {
					log.Printf("Warning: Failed to connect to NATS: %v", err)
				} else {
					if err := natsListener.Subscribe(); err != nil {
						log.Printf("Warning: Failed to subscribe to NATS: %v", err)
					} else {
						log.Println("✅ NATS integration started successfully")
					}
				}
			}
		} else {
			log.Println("Warning: Could not get controller from service, NATS disabled")
		}
	}

	// Disconnect NATS on shutdown
	if natsListener != nil {
		defer natsListener.Disconnect()
	}

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	// Wait for interrupt
	<-stopChan
	log.Println("Shutting down server...")

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err = shutdownFunc(ctx); err != nil {
		log.Printf("Server shutdown error: %v", err)
	}

	log.Println("Server gracefully stopped")
}
