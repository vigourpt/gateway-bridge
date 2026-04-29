#!/usr/bin/env node
/**
 * Gateway Bridge - WebSocket proxy for Mission Control
 * 
 * Allows MC (abacus-mc.vigourclaw.cloud) to connect to multiple OpenClaw gateways
 * and control agents on each one.
 * 
 * Usage:
 *   node index.js                    # Default (connects to local + Thor's gateway)
 *   node index.js --thor-url URL     # Custom Thor gateway URL
 *   node index.js --port PORT        # Custom port (default: 3001)
 */

const WebSocket = require('ws');

// Configuration
const PORT = process.env.PORT || 3001;
const THOR_URL = process.env.THOR_GATEWAY_URL || 'ws://187.124.114.207:45397';
const THOR_TOKEN = process.env.THOR_GATEWAY_TOKEN || 'MzLeAvE5uphx5w6WzwFHxJQdP1s4OalJ';
const LOCAL_GATEWAY_URL = process.env.LOCAL_GATEWAY_URL || 'ws://localhost:45397';

// Gateway registry
const gateways = {
  local: {
    name: 'Jarvis',
    url: LOCAL_GATEWAY_URL,
    token: process.env.LOCAL_GATEWAY_TOKEN || '',
    connected: false,
    clients: new Set()
  },
  thor: {
    name: 'Thor',
    url: THOR_URL,
    token: THOR_TOKEN,
    connected: false,
    clients: new Set()
  }
};

// Connected gateway connections
const gatewayConnections = {};

// Create WebSocket server
const wss = new WebSocket.Server({ port: PORT });

console.log(`🚀 Gateway Bridge starting on port ${PORT}`);
console.log(`   Thor gateway: ${THOR_URL}`);

// Handle new client connections
wss.on('connection', (ws, req) => {
  const clientId = Math.random().toString(36).substring(7);
  console.log(`[Client ${clientId}] Connected`);
  
  // Subscribe to specific gateway via message
  let subscribedGateway = null;
  
  ws.on('message', (message) => {
    try {
      const msg = JSON.parse(message);
      
      // Handle subscription
      if (msg.type === 'subscribe') {
        const gateway = msg.gateway || 'all';
        if (gateway === 'all') {
          // Subscribe to all gateways
          Object.keys(gateways).forEach(gw => {
            gateways[gw].clients.add(clientId);
          });
          console.log(`[Client ${clientId}] Subscribed to all gateways`);
        } else if (gateways[gateway]) {
          gateways[gateway].clients.add(clientId);
          subscribedGateway = gateway;
          console.log(`[Client ${clientId}] Subscribed to ${gateway}`);
          
          // Send current status
          ws.send(JSON.stringify({
            type: 'status',
            gateway,
            connected: gatewayConnections[gateway] ? true : false,
            agentCount: 0
          }));
        }
        return;
      }
      
      // Handle gateway commands
      if (msg.type === 'command') {
        const targetGateway = msg.gateway || subscribedGateway;
        if (!targetGateway || !gatewayConnections[targetGateway]) {
          ws.send(JSON.stringify({
            type: 'error',
            message: `Gateway ${targetGateway} not connected`
          }));
          return;
        }
        
        // Forward command to gateway
        const payload = JSON.stringify({
          type: 'agent_command',
          agentId: msg.agentId,
          command: msg.command,
          args: msg.args || []
        });
        
        gatewayConnections[targetGateway].send(payload);
        console.log(`[Client ${clientId}] Command forwarded to ${targetGateway}`);
        return;
      }
      
      // Handle list agents request
      if (msg.type === 'list_agents') {
        const targetGateway = msg.gateway || 'all';
        
        if (targetGateway === 'all') {
          // Get agents from all gateways
          Promise.all([
            fetchAgents('local'),
            fetchAgents('thor')
          ]).then(results => {
            const allAgents = [...results[0], ...results[1]];
            ws.send(JSON.stringify({
              type: 'agents_list',
              agents: allAgents
            }));
          });
        } else if (gateways[targetGateway]) {
          fetchAgents(targetGateway).then(agents => {
            ws.send(JSON.stringify({
              type: 'agents_list',
              gateway: targetGateway,
              agents
            }));
          });
        }
        return;
      }
      
    } catch (e) {
      console.error(`[Client ${clientId}] Error parsing message:`, e);
    }
  });
  
  ws.on('close', () => {
    console.log(`[Client ${clientId}] Disconnected`);
    // Remove from all gateway subscriptions
    Object.values(gateways).forEach(gw => {
      gw.clients.delete(clientId);
    });
  });
  
  ws.on('error', (err) => {
    console.error(`[Client ${clientId}] Error:`, err.message);
  });
});

// Connect to gateways
async function connectToGateway(gatewayKey) {
  const config = gateways[gatewayKey];
  
  return new Promise((resolve, reject) => {
    console.log(`Connecting to ${config.name} gateway at ${config.url}...`);
    
    const ws = new WebSocket(config.url, {
      headers: config.token ? { 'Authorization': `Bearer ${config.token}` } : {}
    });
    
    ws.on('open', () => {
      console.log(`✅ ${config.name} gateway connected`);
      gatewayConnections[gatewayKey] = ws;
      config.connected = true;
      
      // Notify subscribed clients
      broadcast(gatewayKey, {
        type: 'gateway_status',
        gateway: gatewayKey,
        name: config.name,
        connected: true
      });
      
      resolve();
    });
    
    ws.on('message', (data) => {
      try {
        const msg = JSON.parse(data);
        
        // Forward agent updates to subscribed clients
        if (msg.type === 'agent_update' || msg.type === 'agent_list') {
          broadcast(gatewayKey, {
            type: 'agent_update',
            gateway: gatewayKey,
            data: msg
          });
        }
      } catch (e) {
        // Raw message, ignore
      }
    });
    
    ws.on('close', () => {
      console.log(`❌ ${config.name} gateway disconnected`);
      config.connected = false;
      delete gatewayConnections[gatewayKey];
      
      broadcast(gatewayKey, {
        type: 'gateway_status',
        gateway: gatewayKey,
        name: config.name,
        connected: false
      });
      
      // Reconnect after 5 seconds
      setTimeout(() => {
        console.log(`Reconnecting to ${config.name}...`);
        connectToGateway(gatewayKey);
      }, 5000);
    });
    
    ws.on('error', (err) => {
      console.error(`⚠️ ${config.name} gateway error:`, err.message);
      config.connected = false;
      reject(err);
    });
    
    // Timeout after 10 seconds
    setTimeout(() => {
      if (!config.connected) {
        console.log(`⚠️ ${config.name} gateway connection timeout`);
        ws.close();
        reject(new Error('Connection timeout'));
      }
    }, 10000);
  });
}

// Fetch agents from a gateway
async function fetchAgents(gatewayKey) {
  // If connected, request agent list
  // This is a simplified implementation
  return [];
}

// Broadcast to clients subscribed to a gateway
function broadcast(gatewayKey, message) {
  const clients = gateways[gatewayKey]?.clients || new Set();
  const msgStr = JSON.stringify(message);
  
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(msgStr);
    }
  });
}

// Broadcast to all clients
function broadcastAll(message) {
  const msgStr = JSON.stringify(message);
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(msgStr);
    }
  });
}

// Start connections
async function start() {
  try {
    // Connect to Thor's gateway
    await connectToGateway('thor');
  } catch (e) {
    console.log('Note: Thor gateway not available (may need Docker network setup on VPS)');
  }
  
  console.log('');
  console.log('📡 Gateway Bridge ready!');
  console.log('');
  console.log('Endpoints:');
  console.log(`  - WebSocket: ws://localhost:${PORT}`);
  console.log('');
  console.log('For MC to connect, set env vars:');
  console.log(`  - THOR_GATEWAY_URL=${THOR_URL}`);
  console.log(`  - THOR_GATEWAY_TOKEN=<token>`);
  console.log('');
}

start();