package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pasarguard/node/common"
	"github.com/pasarguard/node/config"
	"github.com/pasarguard/node/controller"
)

// EventType represents the type of event received from panel
type EventType string

const (
	EventUserCreated        EventType = "user.created"
	EventUserUpdated        EventType = "user.updated"
	EventUserDeleted        EventType = "user.deleted"
	EventUserStatusChanged  EventType = "user.status_changed"
	EventXrayConfigUpdated  EventType = "xray.config_updated"
	EventNodeSyncRequested  EventType = "node.sync_requested"
)

// PanelMessage represents a message from the panel
type PanelMessage struct {
	Type      string                 `json:"type"`
	Timestamp string                 `json:"timestamp"`
	Payload   map[string]interface{} `json:"payload"`
}

// UserPayload represents user data in the message
type UserPayload struct {
	ID            int                    `json:"id"`
	Username      string                 `json:"username"`
	Status        string                 `json:"status"`
	ProxySettings map[string]interface{} `json:"proxy_settings"`
	Inbounds      []string               `json:"inbounds"`
}

// NATSListener handles NATS subscriptions for the node
type NATSListener struct {
	nc         *nats.Conn
	cfg        *config.Config
	controller *controller.Controller
	ctx        context.Context
	cancel     context.CancelFunc
}

// New creates a new NATS listener
func New(cfg *config.Config, ctrl *controller.Controller) (*NATSListener, error) {
	if !cfg.NATSEnabled {
		log.Println("NATS is disabled, skipping connection")
		return nil, nil
	}

	ctx, cancel := context.WithCancel(context.Background())

	listener := &NATSListener{
		cfg:        cfg,
		controller: ctrl,
		ctx:        ctx,
		cancel:     cancel,
	}

	return listener, nil
}

// Connect connects to the NATS server
func (nl *NATSListener) Connect() error {
	if !nl.cfg.NATSEnabled {
		return nil
	}

	log.Println("Connecting to NATS server...")

	// Prepare connection options
	opts := []nats.Option{
		nats.Name(fmt.Sprintf("pasarguard-node-%s", nl.cfg.NodeID)),
		nats.MaxReconnects(-1),                        // Infinite reconnect attempts
		nats.ReconnectWait(2 * time.Second),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			log.Printf("NATS disconnected: %v", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Println("NATS reconnected successfully")
		}),
	}

	// Add authentication if token is provided
	if nl.cfg.NATSAuthToken != "" {
		opts = append(opts, nats.Token(nl.cfg.NATSAuthToken))
	}

	// Add TLS if enabled
	if nl.cfg.NATSUseTLS {
		if nl.cfg.NATSTLSCert != "" && nl.cfg.NATSTLSKey != "" {
			opts = append(opts, nats.ClientCert(nl.cfg.NATSTLSCert, nl.cfg.NATSTLSKey))
		}
		if nl.cfg.NATSTLSCa != "" {
			opts = append(opts, nats.RootCAs(nl.cfg.NATSTLSCa))
		}
	}

	// Connect to NATS
	nc, err := nats.Connect(nl.cfg.NATSServers, opts...)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %v", err)
	}

	nl.nc = nc
	log.Printf("✅ Connected to NATS at %s", nl.cfg.NATSServers)

	return nil
}

// Subscribe starts listening for messages from the panel
func (nl *NATSListener) Subscribe() error {
	if !nl.cfg.NATSEnabled || nl.nc == nil {
		return nil
	}

	// Subscribe to node-specific channel
	nodeSubject := fmt.Sprintf("panel.node.%s", nl.cfg.NodeID)
	_, err := nl.nc.Subscribe(nodeSubject, nl.handleMessage)
	if err != nil {
		return fmt.Errorf("failed to subscribe to %s: %v", nodeSubject, err)
	}
	log.Printf("🟢 Subscribed to: %s", nodeSubject)

	// Subscribe to broadcast channel
	broadcastSubject := "panel.broadcast"
	_, err = nl.nc.Subscribe(broadcastSubject, nl.handleMessage)
	if err != nil {
		return fmt.Errorf("failed to subscribe to %s: %v", broadcastSubject, err)
	}
	log.Printf("🟢 Subscribed to: %s", broadcastSubject)

	return nil
}

// handleMessage processes incoming NATS messages
func (nl *NATSListener) handleMessage(msg *nats.Msg) {
	var panelMsg PanelMessage
	if err := json.Unmarshal(msg.Data, &panelMsg); err != nil {
		log.Printf("❌ Failed to parse message: %v", err)
		return
	}

	log.Printf("📩 Received event: %s", panelMsg.Type)

	// Handle different event types
	switch EventType(panelMsg.Type) {
	case EventUserCreated, EventUserUpdated:
		nl.handleUserUpdate(panelMsg.Payload)
	case EventUserDeleted:
		nl.handleUserDelete(panelMsg.Payload)
	case EventUserStatusChanged:
		nl.handleUserStatusChange(panelMsg.Payload)
	case EventXrayConfigUpdated:
		nl.handleConfigUpdate(panelMsg.Payload)
	case EventNodeSyncRequested:
		nl.handleSyncRequest(panelMsg.Payload)
	default:
		log.Printf("⚠️ Unknown event type: %s", panelMsg.Type)
	}
}

// handleUserUpdate handles user creation/update events
func (nl *NATSListener) handleUserUpdate(payload map[string]interface{}) {
	// Parse user data
	userJSON, _ := json.Marshal(payload)
	var user UserPayload
	if err := json.Unmarshal(userJSON, &user); err != nil {
		log.Printf("❌ Failed to parse user payload: %v", err)
		return
	}

	log.Printf("✅ Updating user: %s (ID: %d, Status: %s)", user.Username, user.ID, user.Status)

	// Get backend instance
	backend := nl.controller.Backend()
	if backend == nil {
		log.Println("⚠️ Backend not initialized, cannot update user")
		return
	}

	// Convert payload to protobuf User
	protoUser := nl.payloadToProtoUser(user)

	// Update user in xray core
	if err := backend.SyncUser(context.Background(), protoUser); err != nil {
		log.Printf("❌ Failed to update user %s: %v", user.Username, err)
	} else {
		log.Printf("✅ User %s updated successfully", user.Username)
	}
}

// handleUserDelete handles user deletion events
func (nl *NATSListener) handleUserDelete(payload map[string]interface{}) {
	username, ok := payload["username"].(string)
	if !ok {
		log.Println("❌ Invalid username in delete payload")
		return
	}

	id, ok := payload["id"].(float64)
	if !ok {
		log.Println("❌ Invalid user ID in delete payload")
		return
	}

	log.Printf("🗑️ Deleting user: %s (ID: %d)", username, int(id))

	backend := nl.controller.Backend()
	if backend == nil {
		log.Println("⚠️ Backend not initialized, cannot delete user")
		return
	}

	// Create user with empty inbounds to remove from xray
	protoUser := &common.User{
		Email:    fmt.Sprintf("%d.%s", int(id), username),
		Proxies:  &common.Proxy{},
		Inbounds: []string{},
	}

	if err := backend.SyncUser(context.Background(), protoUser); err != nil {
		log.Printf("❌ Failed to delete user %s: %v", username, err)
	} else {
		log.Printf("✅ User %s deleted successfully", username)
	}
}

// handleUserStatusChange handles user status change events
func (nl *NATSListener) handleUserStatusChange(payload map[string]interface{}) {
	// Status changes are handled via handleUserUpdate
	nl.handleUserUpdate(payload)
}

// handleConfigUpdate handles xray config update events
func (nl *NATSListener) handleConfigUpdate(payload map[string]interface{}) {
	log.Println("📝 Xray config update requested")
	// Implementation depends on how you want to handle config updates
	// This might require restarting the backend with new config
}

// handleSyncRequest handles sync request events
func (nl *NATSListener) handleSyncRequest(payload map[string]interface{}) {
	log.Println("🔄 Node sync requested")
	// Implementation for syncing all users with panel
}

// payloadToProtoUser converts a UserPayload to protobuf User
func (nl *NATSListener) payloadToProtoUser(user UserPayload) *common.User {
	proxies := &common.Proxy{}

	// Extract proxy settings
	if vmess, ok := user.ProxySettings["vmess"].(map[string]interface{}); ok {
		if id, ok := vmess["id"].(string); ok {
			proxies.Vmess = &common.Vmess{Id: id}
		}
	}

	if vless, ok := user.ProxySettings["vless"].(map[string]interface{}); ok {
		vlessMsg := &common.Vless{}
		if id, ok := vless["id"].(string); ok {
			vlessMsg.Id = id
		}
		if flow, ok := vless["flow"].(string); ok {
			vlessMsg.Flow = flow
		}
		proxies.Vless = vlessMsg
	}

	if trojan, ok := user.ProxySettings["trojan"].(map[string]interface{}); ok {
		if password, ok := trojan["password"].(string); ok {
			proxies.Trojan = &common.Trojan{Password: password}
		}
	}

	if ss, ok := user.ProxySettings["shadowsocks"].(map[string]interface{}); ok {
		ssMsg := &common.Shadowsocks{}
		if password, ok := ss["password"].(string); ok {
			ssMsg.Password = password
		}
		if method, ok := ss["method"].(string); ok {
			ssMsg.Method = method
		}
		proxies.Shadowsocks = ssMsg
	}

	// Build inbounds list
	inbounds := user.Inbounds
	if user.Status != "active" && user.Status != "on_hold" {
		// If user is not active, remove from all inbounds
		inbounds = []string{}
	}

	return &common.User{
		Email:    fmt.Sprintf("%d.%s", user.ID, user.Username),
		Proxies:  proxies,
		Inbounds: inbounds,
	}
}

// Disconnect closes the NATS connection
func (nl *NATSListener) Disconnect() {
	if nl.cancel != nil {
		nl.cancel()
	}

	if nl.nc != nil && nl.nc.IsConnected() {
		nl.nc.Drain()
		nl.nc.Close()
		log.Println("Disconnected from NATS")
	}
}

// IsConnected returns whether the NATS connection is active
func (nl *NATSListener) IsConnected() bool {
	return nl.nc != nil && nl.nc.IsConnected()
}

