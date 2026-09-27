const express = require('express');
const cors = require('cors');
const WebSocket = require('ws');
const http = require('http');

// ==================== GAME WEBSOCKET CLIENT ====================
class GameWebSocketClient {
  constructor(url) {
    this.url = url;
    this.websocket = null;
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = 5;
    this.reconnectDelay = 5000; // milliseconds
    this.isAuthenticated = false;
    this.sessionId = null;
    this.latestTxData = null; // Dữ liệu bàn tài xỉu thường (cmd 1005)
    this.latestMd5Data = null; // Dữ liệu bàn MD5 (cmd 1105)
    this.lastUpdateTime = {
      tx: null,
      md5: null
    };
    this.running = true;
  }

  async connect() {
    console.log('🔗 Connecting to WebSocket server...');

    const headers = {
      'Host': 'apisao.net',
      'Origin': 'https://play.sao789a.me',
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
      'Pragma': 'no-cache',
      'Cache-Control': 'no-cache',
      'Accept-Encoding': 'gzip, deflate, br, zstd',
      'Accept-Language': 'vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5'
    };

    try {
      this.websocket = new WebSocket(this.url, {
        headers,
        perMessageDeflate: true
      });

      this.websocket.on('open', async () => {
        this.reconnectAttempts = 0;
        console.log('✅ Connected to WebSocket server');

        // Send authentication
        await this.sendAuthentication();

        // Send plugin messages after a delay
        await this.sleep(2000);
        await this.sendPluginMessages();

        // Start heartbeat and refresh tasks
        this.startHeartbeat();
        this.startRefreshGameData();
      });

      this.websocket.on('message', async (data) => {
        await this.processMessage(data);
      });

      this.websocket.on('error', (error) => {
        console.error('❌ WebSocket error:', error.message);
      });

      this.websocket.on('close', async () => {
        console.log('❌ WebSocket closed');
        await this.handleReconnect();
      });
    } catch (error) {
      console.error('❌ WebSocket connection error:', error.message);
      await this.handleReconnect();
    }
  }

  async sendAuthentication() {
    console.log('🔐 Sending authentication...');

    const authMessage = [
      1,
      "MiniGame",
      "kietdz2012",
      "kiet2012",
      {
        "pid": 4,
        "cs": "91f9a218a96b2f32bb0eeb57dc462447",
        "phone": "84983994667",
        "deviceId": "1744875827603",
        "ipAddress": "2402:800:62cd:df12:c47d:24e3:2791:bd61",
        "isMerchant": false,
        "isMktAccount": false,
        "timestamp": 1778060539932,
        "userId": "cbcb534b-7f75-480b-9a5d-a72f8d53adb8",
        "username": "kietdz2012",
        "signature": "51D933DBECABDE84111AB2FCC22E04A06FDE3AD41A6DCAF59F495A5158BF69972F9F4FE7C9D877A669ED06FAFEFB9AB7E92664D4648B724399D84C513534780E13073E1AAC746A909BD67DE18477D770E47C350E629CDC7B0F9F8CFE53140392BC72A2FDB56D3037B536D8A0BEF3722DBBBD2797116CB4833714DC4B81D8C524"
      }
    ];

    await this.sendRaw(authMessage);
    this.isAuthenticated = true;
  }

  async sendPluginMessages() {
    console.log('🚀 Sending plugin initialization messages...');

    const pluginMessages = [
      [
        6,
        "MiniGame",
        "taixiuPlugin",
        {
          "cmd": 1005
        }
      ],
      [
        6,
        "MiniGame",
        "taixiuMd5Plugin",
        {
          "cmd": 1105
        }
      ],
      [
        6,
        "MiniGame",
        "taixiuLiveRoomPlugin",
        {
          "cmd": 1305,
          "rid": 0
        }
      ],
      [
        6,
        "MiniGame",
        "taixiuMd5v2Plugin",
        {
          "cmd": 1405
        }
      ],
      [
        6,
        "MiniGame",
        "lobbyPlugin",
        {
          "cmd": 10001
        }
      ]
    ];

    for (let index = 0; index < pluginMessages.length; index++) {
      await this.sleep(1000); // Delay 1 second between messages
      console.log(`📤 Sending plugin ${index + 1}/${pluginMessages.length}: ${pluginMessages[index][2]}`);
      await this.sendRaw(pluginMessages[index]);
    }
  }

  startRefreshGameData() {
    setInterval(async () => {
      if (this.isAuthenticated && this.websocket && this.websocket.readyState === WebSocket.OPEN) {
        console.log('🔄 Refreshing game data...');

        const refreshTx = [
          6,
          "MiniGame",
          "taixiuPlugin",
          {
            "cmd": 1005
          }
        ];

        const refreshMd5 = [
          6,
          "MiniGame",
          "taixiuMd5Plugin",
          {
            "cmd": 1105
          }
        ];

        await this.sendRaw(refreshTx);
        await this.sleep(1000);
        await this.sendRaw(refreshMd5);
      }
    }, 30000); // Refresh every 30 seconds
  }

  async sendRaw(data) {
    if (this.websocket && this.websocket.readyState === WebSocket.OPEN) {
      const jsonString = JSON.stringify(data);
      this.websocket.send(jsonString);
      console.log('📤 Sent raw:', jsonString);
      return true;
    } else {
      console.warn('⚠️ Cannot send, WebSocket not open');
      return false;
    }
  }

  async processMessage(data) {
    try {
      const parsed = JSON.parse(data);

      // XỬ LÝ CMD 1005 - BÀN TÀI XỈU THƯỜNG
      if (parsed.length > 1 && typeof parsed[1] === 'object' && parsed[1].cmd === 1005) {
        console.log('🎯 Nhận được dữ liệu cmd 1005 (Bàn TX)');

        const gameData = parsed[1];

        if (gameData.htr && gameData.htr.length > 0) {
          const latestSession = gameData.htr.reduce((max, current) =>
            current.sid > max.sid ? current : max
          );

          console.log(
            `🎲 Bàn TX - Phiên gần nhất: ${latestSession.sid} (${latestSession.d1},${latestSession.d2},${latestSession.d3})`
          );

          this.latestTxData = gameData;
          this.lastUpdateTime.tx = new Date();
          console.log('💾 Đã cập nhật dữ liệu bàn TX');
        }
      }

      // XỬ LÝ CMD 1105 - BÀN MD5
      else if (parsed.length > 1 && typeof parsed[1] === 'object' && parsed[1].cmd === 1105) {
        console.log('🎯 Nhận được dữ liệu cmd 1105 (Bàn MD5)');

        const gameData = parsed[1];

        if (gameData.htr && gameData.htr.length > 0) {
          const latestSession = gameData.htr.reduce((max, current) =>
            current.sid > max.sid ? current : max
          );

          console.log(
            `🎲 Bàn MD5 - Phiên gần nhất: ${latestSession.sid} (${latestSession.d1},${latestSession.d2},${latestSession.d3})`
          );

          this.latestMd5Data = gameData;
          this.lastUpdateTime.md5 = new Date();
          console.log('💾 Đã cập nhật dữ liệu bàn MD5');
        }
      }

      // Xử lý các lệnh khác
      else if (parsed.length > 0 && parsed[0] !== 0) {
        console.log('📨 Received:', JSON.stringify(parsed).substring(0, 200));
      }
    } catch (error) {
      console.error('❌ Error processing message:', error.message);
    }
  }

  getLatestTxSession() {
    if (!this.latestTxData || !this.latestTxData.htr || this.latestTxData.htr.length === 0) {
      return {
        error: "Không có dữ liệu bàn TX",
        message: "Chưa nhận được dữ liệu từ server hoặc dữ liệu trống"
      };
    }

    try {
      const latestSession = this.latestTxData.htr.reduce((max, current) =>
        current.sid > max.sid ? current : max
      );

      const tong = latestSession.d1 + latestSession.d2 + latestSession.d3;
      const ketQua = tong >= 11 && tong <= 18 ? "tài" : "xỉu";

      return {
        phien: latestSession.sid,
        xuc_xac_1: latestSession.d1,
        xuc_xac_2: latestSession.d2,
        xuc_xac_3: latestSession.d3,
        tong: tong,
        ket_qua: ketQua,
        timestamp: new Date().toISOString(),
        ban: "tai_xiu",
        last_updated: this.lastUpdateTime.tx ? this.lastUpdateTime.tx.toISOString() : null
      };
    } catch (error) {
      return {
        error: "Lỗi xử lý dữ liệu TX",
        message: error.message
      };
    }
  }

  getLatestMd5Session() {
    if (!this.latestMd5Data || !this.latestMd5Data.htr || this.latestMd5Data.htr.length === 0) {
      return {
        error: "Không có dữ liệu bàn MD5",
        message: "Chưa nhận được dữ liệu từ server hoặc dữ liệu trống"
      };
    }

    try {
      const latestSession = this.latestMd5Data.htr.reduce((max, current) =>
        current.sid > max.sid ? current : max
      );

      const tong = latestSession.d1 + latestSession.d2 + latestSession.d3;
      const ketQua = tong >= 11 && tong <= 18 ? "tài" : "xỉu";

      return {
        phien: latestSession.sid,
        xuc_xac_1: latestSession.d1,
        xuc_xac_2: latestSession.d2,
        xuc_xac_3: latestSession.d3,
        tong: tong,
        ket_qua: ketQua,
        timestamp: new Date().toISOString(),
        ban: "md5",
        last_updated: this.lastUpdateTime.md5 ? this.lastUpdateTime.md5.toISOString() : null
      };
    } catch (error) {
      return {
        error: "Lỗi xử lý dữ liệu MD5",
        message: error.message
      };
    }
  }

  async handleReconnect() {
    if (this.reconnectAttempts < this.maxReconnectAttempts) {
      this.reconnectAttempts++;
      const delay = this.reconnectDelay * this.reconnectAttempts;

      console.log(
        `🔄 Attempting to reconnect in ${delay / 1000}s (Attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts})`
      );

      await this.sleep(delay);
      console.log('🔄 Reconnecting...');
      await this.connect();
    } else {
      console.error('❌ Max reconnection attempts reached');
    }
  }

  startHeartbeat() {
    setInterval(async () => {
      if (this.websocket && this.websocket.readyState === WebSocket.OPEN) {
        const heartbeatMsg = [0, this.sessionId || ""];
        this.websocket.send(JSON.stringify(heartbeatMsg));
        console.log('❤️ Sending heartbeat...');
      }
    }, 25000);
  }

  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  close() {
    this.running = false;
    if (this.websocket) {
      this.websocket.close();
    }
  }
}

// ==================== EXPRESS SERVER ====================
const app = express();
const PORT = process.env.PORT || 3012;

// Middleware
app.use(cors());
app.use(express.json());

// Create WebSocket client
const client = new GameWebSocketClient(
  'wss://apisao.net/websocket?d=YTIxaWNHNWtiMjg9fDU1fDE3NzgwNjA1MzcwMjR8NjVlYWNmNmI3ZjhhYzY1NTE4YzIyMjJiMmRmNDU4Nzh8YWRlY2VjMWIyNjY5MzJmYzRlMGIzMjVjYjk3NWFiNjJ8Mjc4QjgxNEE='
);

// Start WebSocket client
client.connect();

// ==================== API ROUTES ====================

// Get TX (Tài Xỉu) latest session
app.get('/api/tx', (req, res) => {
  try {
    const latestSession = client.getLatestTxSession();

    if (latestSession.error) {
      return res.status(404).json(latestSession);
    }

    res.json(latestSession);
  } catch (error) {
    res.status(500).json({
      error: "Lỗi server",
      message: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// Get MD5 latest session
app.get('/api/md5', (req, res) => {
  try {
    const latestSession = client.getLatestMd5Session();

    if (latestSession.error) {
      return res.status(404).json(latestSession);
    }

    res.json(latestSession);
  } catch (error) {
    res.status(500).json({
      error: "Lỗi server",
      message: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// Get both TX and MD5
app.get('/api/all', (req, res) => {
  try {
    const txSession = client.getLatestTxSession();
    const md5Session = client.getLatestMd5Session();

    res.json({
      tai_xiu: txSession.error ? { error: txSession.error } : txSession,
      md5: md5Session.error ? { error: md5Session.error } : md5Session,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    res.status(500).json({
      error: "Lỗi server",
      message: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// Get server status
app.get('/api/status', (req, res) => {
  const hasTxData = client.latestTxData && client.latestTxData.htr && client.latestTxData.htr.length > 0;
  const hasMd5Data = client.latestMd5Data && client.latestMd5Data.htr && client.latestMd5Data.htr.length > 0;

  let txLatestSid = null;
  if (hasTxData) {
    txLatestSid = client.latestTxData.htr.reduce((max, current) =>
      current.sid > max.sid ? current : max
    ).sid;
  }

  let md5LatestSid = null;
  if (hasMd5Data) {
    md5LatestSid = client.latestMd5Data.htr.reduce((max, current) =>
      current.sid > max.sid ? current : max
    ).sid;
  }

  res.json({
    status: "running",
    websocket_connected: client.websocket && client.websocket.readyState === WebSocket.OPEN,
    authenticated: client.isAuthenticated,
    has_tx_data: hasTxData,
    has_md5_data: hasMd5Data,
    tx_data_count: hasTxData ? client.latestTxData.htr.length : 0,
    md5_data_count: hasMd5Data ? client.latestMd5Data.htr.length : 0,
    tx_latest_sid: txLatestSid,
    md5_latest_sid: md5LatestSid,
    tx_last_updated: client.lastUpdateTime.tx ? client.lastUpdateTime.tx.toISOString() : null,
    md5_last_updated: client.lastUpdateTime.md5 ? client.lastUpdateTime.md5.toISOString() : null,
    timestamp: new Date().toISOString()
  });
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Root endpoint
app.get('/', (req, res) => {
  res.json({
    message: '🎲 @dongnetsun SAO Game API Server',
    author: '@dongnetsun',
    endpoints: {
      'GET /api/tx': 'Get latest TX (Tài Xỉu) session',
      'GET /api/md5': 'Get latest MD5 session',
      'GET /api/all': 'Get both TX and MD5 sessions',
      'GET /api/status': 'Get server status',
      'GET /health': 'Health check'
    },
    timestamp: new Date().toISOString()
  });
});

// ==================== START SERVER ====================
app.listen(PORT, '0.0.0.0', () => {
  console.log(`\n🚀 @dongnetsun Server running at: http://0.0.0.0:${PORT}`);
  console.log(`🎲 @dongnetsun API TX: http://localhost:${PORT}/api/tx`);
  console.log(`🔐 @dongnetsun API MD5: http://localhost:${PORT}/api/md5`);
  console.log(`📊 @dongnetsun API Both: http://localhost:${PORT}/api/all`);
  console.log(`📡 @dongnetsun Status: http://localhost:${PORT}/api/status`);
  console.log(`❤️ @dongnetsun Health: http://localhost:${PORT}/health\n`);
});

// Handle graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n👋 Closing WebSocket connection and server...');
  await client.close();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('\n👋 Closing WebSocket connection and server...');
  await client.close();
  process.exit(0);
});
