const MAX_MESSAGE_SIZE = 65536;
const ROOM_PATTERN = /^[A-Z0-9]{4,12}$/;

export default {
    async fetch(request, env) {
        const url = new URL(request.url);

        // Health check
        if (url.pathname === "/health") {
            return Response.json({
                status: "online",
                service: "imposter-relay",
                timestamp: Date.now()
            });
        }

        // WebSocket endpoint
        if (url.pathname !== "/ws") {
            return new Response("Not Found", { status: 404 });
        }

        if (request.method !== "GET") {
            return new Response("Method Not Allowed", { status: 405 });
        }

        if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
            return new Response("WebSocket upgrade required", { status: 426 });
        }

        const roomId = (url.searchParams.get("room") || "").toUpperCase();
        const role = url.searchParams.get("role");
        const playerId = url.searchParams.get("playerId") || "";

        if (!ROOM_PATTERN.test(roomId)) {
            return new Response("Invalid room ID", { status: 400 });
        }

        if (role !== "host" && role !== "player") {
            return new Response("Invalid connection role", { status: 400 });
        }

        if (
            role === "player" &&
            (!/^[a-zA-Z0-9_-]{1,40}$/.test(playerId))
        ) {
            return new Response("Invalid player ID", { status: 400 });
        }

        const id = env.GAME_ROOMS.idFromName(roomId);
        const room = env.GAME_ROOMS.get(id);

        return room.fetch(request);
    }
};

export class GameRoom {
    constructor(ctx, env) {
        this.ctx = ctx;
        this.env = env;
    }

    async fetch(request) {
        const url = new URL(request.url);

        const roomId = (url.searchParams.get("room") || "").toUpperCase();
        const role = url.searchParams.get("role");
        const playerId = url.searchParams.get("playerId") || "";

        if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
            return new Response("WebSocket upgrade required", {
                status: 426
            });
        }

        // Host authentication
        if (role === "host") {
            const token = request.headers.get("X-Host-Token");

            if (!token || token.length < 24) {
                return new Response("Invalid host authentication", {
                    status: 401
                });
            }

            const authenticated = await this.ctx.storage.transaction(
                async (transaction) => {
                    const savedToken = await transaction.get("hostToken");

                    if (!savedToken) {
                        await transaction.put("hostToken", token);
                        return true;
                    }

                    return savedToken === token;
                }
            );

            if (!authenticated) {
                return new Response("Host authentication failed", {
                    status: 403
                });
            }

            if (this.ctx.getWebSockets("host").length > 0) {
                return new Response("Host already connected", {
                    status: 409
                });
            }
        }

        if (role !== "host" && role !== "player") {
            return new Response("Invalid role", { status: 400 });
        }

        // Create WebSocket connection
        const pair = new WebSocketPair();

        const client = pair[0];
        const server = pair[1];

        const tags = role === "host"
            ? ["host"]
            : ["player", playerId];

        this.ctx.acceptWebSocket(server, tags);

        // Notify the newly connected client
        this.send(server, {
            type: "CONNECTED",
            roomId,
            role,
            playerId: role === "player" ? playerId : null
        });

        // Notify host when a player joins
        if (role === "player") {
            this.sendToHost({
                type: "PLAYER_CONNECTED",
                roomId,
                playerId
            });
        }

        return new Response(null, {
            status: 101,
            webSocket: client
        });
    }

    // Handle incoming messages
    async webSocketMessage(ws, message) {
        if (typeof message !== "string") {
            this.send(ws, {
                type: "ERROR",
                message: "Only JSON text messages are supported"
            });
            return;
        }

        if (message.length > MAX_MESSAGE_SIZE) {
            this.send(ws, {
                type: "ERROR",
                message: "Message too large"
            });
            return;
        }

        let data;

        try {
            data = JSON.parse(message);
        } catch {
            this.send(ws, {
                type: "ERROR",
                message: "Invalid JSON"
            });
            return;
        }

        if (
            !data ||
            typeof data !== "object" ||
            Array.isArray(data) ||
            typeof data.type !== "string"
        ) {
            this.send(ws, {
                type: "ERROR",
                message: "Invalid message format"
            });
            return;
        }

        const tags = ws.getTags();
        const role = tags.includes("host") ? "host" : "player";
        const playerId = role === "player"
            ? tags.find(tag => tag !== "player") || ""
            : null;

        // Heartbeat
        if (data.type === "PING") {
            this.send(ws, {
                type: "PONG",
                timestamp: Date.now()
            });
            return;
        }

        const outgoing = JSON.stringify({
            ...data,
            _relay: {
                role,
                playerId,
                timestamp: Date.now()
            }
        });

        // Host -> all connected players
        if (role === "host") {
            this.broadcastToPlayers(outgoing);
            return;
        }

        // Player -> Android host
        this.sendToHostRaw(outgoing, ws);
    }

    // Send a message to the host
    sendToHost(data) {
        const message = JSON.stringify(data);

        for (const socket of this.ctx.getWebSockets("host")) {
            this.sendRaw(socket, message);
        }
    }

    // Forward player messages to host
    sendToHostRaw(message, sender) {
        const hosts = this.ctx.getWebSockets("host");

        if (hosts.length === 0) {
            this.send(sender, {
                type: "ERROR",
                message: "Game host is disconnected"
            });
            return;
        }

        for (const socket of hosts) {
            this.sendRaw(socket, message);
        }
    }

    // Broadcast host messages to all players
    broadcastToPlayers(message) {
        for (const socket of this.ctx.getWebSockets("player")) {
            this.sendRaw(socket, message);
        }
    }

    // Send JSON
    send(ws, data) {
        this.sendRaw(ws, JSON.stringify(data));
    }

    // Send raw text
    sendRaw(ws, message) {
        try {
            ws.send(message);
        } catch (error) {
            console.error("WebSocket send failed:", error);
        }
    }

    // Handle disconnects
    async webSocketClose(ws, code, reason, wasClean) {
        const tags = ws.getTags();

        if (tags.includes("host")) {
            this.broadcastToPlayers(JSON.stringify({
                type: "HOST_DISCONNECTED",
                timestamp: Date.now()
            }));

            return;
        }

        const playerId = tags.find(tag => tag !== "player");

        this.sendToHost({
            type: "PLAYER_DISCONNECTED",
            playerId,
            timestamp: Date.now()
        });
    }

    // Handle socket errors
    async webSocketError(ws, error) {
        console.error("WebSocket error:", error);
    }
}
