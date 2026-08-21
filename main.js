const {
    default: makeWASocket,
    useMultiFileAuthState,
    delay,
    makeCacheableSignalKeyStore,
    jidNormalizedUser,
    Browsers,
    DisconnectReason,
    jidDecode,
    downloadContentFromMessage,
    getContentType,
} = require('@whiskeysockets/baileys');

const { arslanmd } = require('./lib/system');
const config = require('./config');
const events = require('./arslan');
const { sms } = require('./lib/msg');

const {
    connectdb,
    saveSessionToMongoDB,
    getSessionFromMongoDB,
    deleteSessionFromMongoDB,
    getUserConfigFromMongoDB,
    updateUserConfigInMongoDB,
    addNumberToMongoDB,
    removeNumberFromMongoDB,
    getAllNumbersFromMongoDB,
    saveOTPToMongoDB,
    verifyOTPFromMongoDB,
    incrementStats,
    getStatsForNumber
} = require('./lib/database');

const { handleAntidelete } = require('./lib/antidelete');

const express = require('express');
const fs = require('fs-extra');
const path = require('path');
const pino = require('pino');
const crypto = require('crypto');
const FileType = require('file-type');
const axios = require('axios');
const moment = require('moment-timezone');

const prefix = config.PREFIX;
const mode = config.MODE || config.WORK_TYPE;
const router = express.Router();

connectdb();

const activeSockets = new Map();
const socketCreationTime = new Map();
const pairingCodes = new Map();
const pairingInProgress = new Map();


/* =========================================================
 * MESSAGE STORE
 * ========================================================= */

function createarslanStore() {
    const store = {
        messages: {},

        bind(ev) {
            ev.on('messages.upsert', ({ messages }) => {
                for (const msg of messages) {
                    const jid = msg.key && msg.key.remoteJid;

                    if (!jid) continue;

                    if (!store.messages[jid]) {
                        store.messages[jid] = [];
                    }

                    store.messages[jid].push(msg);

                    if (store.messages[jid].length > 200) {
                        store.messages[jid].shift();
                    }
                }
            });
        },

        async loadMessage(jid, id) {
            if (!store.messages[jid]) {
                return null;
            }

            return (
                store.messages[jid].find(
                    m => m.key && m.key.id === id
                ) || null
            );
        }
    };

    return store;
}


/* =========================================================
 * UTILITIES
 * ========================================================= */

const createSerial = size =>
    crypto.randomBytes(size).toString('hex').slice(0, size);


const getGroupAdmins = participants => {
    const admins = [];

    for (const participant of participants || []) {
        if (participant.admin == null) continue;

        admins.push(participant.id);
    }

    return admins;
};


function sanitizeNumber(number) {
    return String(number || '').replace(/[^0-9]/g, '');
}


function isNumberAlreadyConnected(number) {
    return activeSockets.has(sanitizeNumber(number));
}


function getConnectionStatus(number) {
    const n = sanitizeNumber(number);

    const isConnected = activeSockets.has(n);
    const connectionTime = socketCreationTime.get(n);

    return {
        isConnected,
        connectionTime: connectionTime
            ? new Date(connectionTime).toLocaleString()
            : null,
        uptime: connectionTime
            ? Math.floor((Date.now() - connectionTime) / 1000)
            : 0
    };
}


function arslanLog(message, type = 'info') {
    const icons = {
        info: '📝',
        success: '✅',
        error: '❌',
        warning: '⚠️',
        debug: '🐛'
    };

    console.log(
        `${icons[type] || '📝'} [ARSLAN-MD] ${new Date().toISOString()}: ${message}`
    );
}


/* =========================================================
 * PLUGINS
 * ========================================================= */

const pluginsDir = path.join(__dirname, 'plugins');

if (!fs.existsSync(pluginsDir)) {
    fs.mkdirSync(pluginsDir, {
        recursive: true
    });
}

const pluginFiles = fs
    .readdirSync(pluginsDir)
    .filter(file => file.endsWith('.js'));

arslanLog(
    `Loading ${pluginFiles.length} plugins...`,
    'info'
);

for (const file of pluginFiles) {
    try {
        require(path.join(pluginsDir, file));

        arslanLog(
            `Loaded plugin: ${file}`,
            'success'
        );
    } catch (e) {
        arslanLog(
            `Failed to load plugin ${file}: ${e.message}`,
            'error'
        );
    }
}


/* =========================================================
 * ANTI CALL
 * ========================================================= */

async function setupCallHandlers(socket, number) {
    socket.ev.on('call', async calls => {
        try {
            const userConfig =
                await getUserConfigFromMongoDB(number);

            if (userConfig?.ANTI_CALL !== 'true') {
                return;
            }

            for (const call of calls || []) {
                if (call.status !== 'offer') {
                    continue;
                }

                await socket.rejectCall(
                    call.id,
                    call.from
                );

                await socket.sendMessage(
                    call.from,
                    {
                        text:
                            userConfig.REJECT_MSG ||
                            config.REJECT_MSG ||
                            'Calls are not allowed.'
                    }
                );

                arslanLog(
                    `Auto-rejected call for ${number} from ${call.from}`,
                    'info'
                );
            }

        } catch (err) {
            arslanLog(
                `Anti-call error for ${number}: ${err.message}`,
                'error'
            );
        }
    });
}


/* =========================================================
 * AUTO RESTART
 * ========================================================= */

function setupAutoRestart(socket, number) {
    let restartAttempts = 0;
    const maxRestartAttempts = 3;

    socket.ev.on(
        'connection.update',
        async update => {
            const {
                connection,
                lastDisconnect
            } = update;

            if (connection === 'open') {
                restartAttempts = 0;
                return;
            }

            if (connection !== 'close') {
                return;
            }

            const disconnectError =
                lastDisconnect?.error;

            const statusCode =
                disconnectError?.output?.statusCode ??
                disconnectError?.statusCode;

            const errorMessage =
                disconnectError?.message ||
                String(
                    disconnectError ||
                    'Unknown disconnect error'
                );

            arslanLog(
                `Connection closed for ${number}: ${statusCode} - ${errorMessage}`,
                'warning'
            );


            /* ================================
             * LOGGED OUT
             * ================================ */

            if (
                statusCode === 401 ||
                errorMessage.includes('401')
            ) {
                arslanLog(
                    `Manual unlink detected for ${number}, cleaning up...`,
                    'warning'
                );

                const sanitizedNumber =
                    sanitizeNumber(number);

                activeSockets.delete(
                    sanitizedNumber
                );

                socketCreationTime.delete(
                    sanitizedNumber
                );

                pairingCodes.delete(
                    sanitizedNumber
                );

                pairingInProgress.delete(
                    sanitizedNumber
                );

                try {
                    await deleteSessionFromMongoDB(
                        sanitizedNumber
                    );
                } catch (e) {
                    arslanLog(
                        `Mongo session delete error: ${e.message}`,
                        'error'
                    );
                }

                try {
                    await removeNumberFromMongoDB(
                        sanitizedNumber
                    );
                } catch (e) {
                    arslanLog(
                        `Mongo number delete error: ${e.message}`,
                        'error'
                    );
                }

                try {
                    socket.ev.removeAllListeners();
                } catch (_) {}

                return;
            }


            /* ================================
             * NORMAL CLOSE
             * ================================ */

            const isNormalError =
                statusCode === 408 ||
                errorMessage.includes(
                    'QR refs attempts ended'
                );

            if (isNormalError) {
                arslanLog(
                    `Normal closure for ${number}, no restart needed.`,
                    'info'
                );

                return;
            }


            /* ================================
             * RECONNECT
             * ================================ */

            if (restartAttempts < maxRestartAttempts) {
                restartAttempts++;

                arslanLog(
                    `Reconnecting ${number} (${restartAttempts}/${maxRestartAttempts}) in 10s...`,
                    'warning'
                );

                const sanitizedNumber =
                    sanitizeNumber(number);

                activeSockets.delete(
                    sanitizedNumber
                );

                socketCreationTime.delete(
                    sanitizedNumber
                );

                try {
                    socket.ev.removeAllListeners();
                } catch (_) {}

                await delay(10000);

                try {
                    const mockRes = {
                        headersSent: false,

                        send: () => {},

                        json: () => {},

                        status() {
                            return mockRes;
                        },

                        setHeader: () => {}
                    };

                    await arslanPair(
                        number,
                        mockRes
                    );

                } catch (e) {
                    arslanLog(
                        `Reconnection failed for ${number}: ${e.message}`,
                        'error'
                    );
                }

            } else {
                arslanLog(
                    `Max restart attempts reached for ${number}.`,
                    'error'
                );
            }
        }
    );
}


/* =========================================================
 * PAIR SOCKET
 * ========================================================= */

async function arslanPair(number, res = null) {
    let connectionLockKey;

    const sanitizedNumber =
        sanitizeNumber(number);

    try {

        if (!sanitizedNumber) {
            if (res && !res.headersSent) {
                return res.status(400).json({
                    error: 'Invalid WhatsApp number'
                });
            }

            return;
        }


        const sessionPath = path.join(
            __dirname,
            'session',
            `session_${sanitizedNumber}`
        );


        /* ================================
         * ALREADY CONNECTED
         * ================================ */

        if (
            isNumberAlreadyConnected(
                sanitizedNumber
            )
        ) {
            const status =
                getConnectionStatus(
                    sanitizedNumber
                );

            if (res && !res.headersSent) {
                return res.json({
                    status: 'already_connected',
                    message:
                        'Number is already connected',
                    connectionTime:
                        status.connectionTime,
                    uptime:
                        `${status.uptime} seconds`
                });
            }

            return;
        }


        /* ================================
         * CONNECTION LOCK
         * ================================ */

        connectionLockKey =
            `arslan_lock_${sanitizedNumber}`;

        if (global[connectionLockKey]) {

            if (res && !res.headersSent) {
                return res.json({
                    status:
                        'connection_in_progress'
                });
            }

            return;
        }

        global[connectionLockKey] = true;


        /* ================================
         * MONGODB SESSION
         * ================================ */

        const existingSession =
            await getSessionFromMongoDB(
                sanitizedNumber
            );


        if (!existingSession) {

            arslanLog(
                `No MongoDB session for ${sanitizedNumber} — new pairing required`,
                'info'
            );

            if (fs.existsSync(sessionPath)) {
                await fs.remove(sessionPath);

                arslanLog(
                    `Cleaned leftover local session for ${sanitizedNumber}`,
                    'info'
                );
            }

        } else {

            fs.ensureDirSync(
                sessionPath
            );

            fs.writeFileSync(
                path.join(
                    sessionPath,
                    'creds.json'
                ),
                JSON.stringify(
                    existingSession,
                    null,
                    2
                )
            );

            arslanLog(
                `🔄 Restored existing session from MongoDB for ${sanitizedNumber}`,
                'success'
            );
        }


        /* ================================
         * AUTH STATE
         * ================================ */

        const {
            state,
            saveCreds
        } = await useMultiFileAuthState(
            sessionPath
        );


        const logger = pino({
            level:
                process.env.NODE_ENV ===
                'production'
                    ? 'fatal'
                    : 'debug'
        });


        const arslanStore =
            createarslanStore();


        /* ================================
         * WHATSAPP SOCKET
         * ================================ */

        const conn = makeWASocket({

            auth: {
                creds: state.creds,

                keys:
                    makeCacheableSignalKeyStore(
                        state.keys,
                        logger
                    )
            },

            printQRInTerminal: false,

            logger: pino({
                level: 'silent'
            }),

            version: [
                2,
                3000,
                1033105955
            ],

            connectTimeoutMs: 60000,

            defaultQueryTimeoutMs: 0,

            keepAliveIntervalMs: 10000,

            emitOwnEvents: true,

            fireInitQueries: true,

            generateHighQualityLinkPreview:
                true,

            syncFullHistory: true,

            markOnlineOnConnect: true,

            browser: Browsers.macOS('Safari'),

            getMessage: async key => {

                const msg =
                    await arslanStore.loadMessage(
                        key.remoteJid,
                        key.id
                    );

                return msg && msg.message
                    ? msg.message
                    : {
                        conversation:
                            'ARSLAN-MD'
                    };
            }
        });


        /* ================================
         * REGISTER ACTIVE SOCKET
         * ================================ */

        socketCreationTime.set(
            sanitizedNumber,
            Date.now()
        );

        activeSockets.set(
            sanitizedNumber,
            conn
        );

        arslanStore.bind(
            conn.ev
        );


        /* ================================
         * HANDLERS
         * ================================ */

        setupCallHandlers(
            conn,
            sanitizedNumber
        );

        setupAutoRestart(
            conn,
            sanitizedNumber
        );


        /* ================================
         * DECODE JID
         * ================================ */

        conn.decodeJid = jid => {

            if (!jid) {
                return jid;
            }

            if (/:\\d+@/gi.test(jid)) {

                const decode =
                    jidDecode(jid) || {};

                return (
                    decode.user &&
                    decode.server
                )
                    ? `${decode.user}@${decode.server}`
                    : jid;
            }

            return jid;
        };


        /* ================================
         * DOWNLOAD MEDIA
         * ================================ */

        conn.downloadAndSaveMediaMessage =
            async (
                message,
                filename,
                attachExtension = true
            ) => {

                const quoted =
                    message.msg
                        ? message.msg
                        : message;

                const mime =
                    (message.msg || message)
                        .mimetype || '';

                const messageType =
                    message.mtype
                        ? message.mtype.replace(
                            /Message/gi,
                            ''
                        )
                        : mime.split('/')[0];

                const stream =
                    await downloadContentFromMessage(
                        quoted,
                        messageType
                    );

                let buffer = Buffer.from([]);

                for await (
                    const chunk of stream
                ) {
                    buffer = Buffer.concat([
                        buffer,
                        chunk
                    ]);
                }

                const type =
                    await FileType.fromBuffer(
                        buffer
                    );

                const trueFileName =
                    attachExtension && type?.ext
                        ? `${filename}.${type.ext}`
                        : filename;

                await fs.writeFile(
                    trueFileName,
                    buffer
                );

                return trueFileName;
            };


        /* =================================================
         * PAIRING CODE
         * ================================================= */

        if (!conn.authState.creds.registered) {

            arslanLog(
                `🔐 Starting NEW pairing process for ${sanitizedNumber}`,
                'info'
            );

            try {

                /* Existing pairing code */

                if (
                    pairingCodes.has(
                        sanitizedNumber
                    )
                ) {

                    const cached =
                        pairingCodes.get(
                            sanitizedNumber
                        );

                    arslanLog(
                        `♻️ Reusing existing pairing code for ${sanitizedNumber}`,
                        'info'
                    );

                    if (
                        res &&
                        !res.headersSent
                    ) {
                        return res.send({
                            code:
                                cached.code,
                            status:
                                'existing_pairing'
                        });
                    }

                    return;
                }


                /* Prevent duplicate requests */

                if (
                    pairingInProgress.has(
                        sanitizedNumber
                    )
                ) {

                    if (
                        res &&
                        !res.headersSent
                    ) {
                        return res.send({
                            status:
                                'pairing_in_progress',

                            message:
                                'Pairing code is already being generated'
                        });
                    }

                    return;
                }


                pairingInProgress.set(
                    sanitizedNumber,
                    true
                );


                await delay(1500);


                const code =
                    await conn.requestPairingCode(
                        sanitizedNumber
                    );


                pairingCodes.set(
                    sanitizedNumber,
                    {
                        code,
                        createdAt:
                            Date.now()
                    }
                );


                arslanLog(
                    `🔐 Pairing Code for ${sanitizedNumber}: ${code}`,
                    'success'
                );


                if (
                    res &&
                    !res.headersSent
                ) {
                    res.send({
                        code,
                        status:
                            'new_pairing'
                    });
                }

            } catch (error) {

                arslanLog(
                    `Failed to request pairing code: ${error.message}`,
                    'error'
                );

                if (
                    res &&
                    !res.headersSent
                ) {
                    res.status(500).send({
                        error:
                            'Failed to get pairing code',

                        status:
                            'error',

                        message:
                            error.message
                    });
                }

            } finally {

                pairingInProgress.delete(
                    sanitizedNumber
                );
            }

        } else {

            arslanLog(
                `✅ Using existing session for ${sanitizedNumber}`,
                'success'
            );

            if (
                res &&
                !res.headersSent
            ) {
                res.json({
                    status:
                        'reconnecting',

                    message:
                        'Reconnecting with existing session'
                });
            }
        }


        /* =================================================
         * CREDS UPDATE
         * ================================================= */
         
        conn.ev.on(
            'creds.update',
            async () => {

                try {

                    await saveCreds();

                    const credsPath =
                        path.join(
                            sessionPath,
                            'creds.json'
                        );

                    if (
                        !fs.existsSync(
                            credsPath
                        )
                    ) {
                        return;
                    }

                    const fileContent =
                        await fs.readFile(
                            credsPath,
                            'utf8'
                        );

                    const creds =
                        JSON.parse(
                            fileContent
                        );

                    const existingSessionCheck =
                        await getSessionFromMongoDB(
                            sanitizedNumber
                        );

                    const isNewSession =
                        !existingSessionCheck;

                    await saveSessionToMongoDB(
                        sanitizedNumber,
                        creds
                    );

                    if (isNewSession) {

                        arslanLog(
                            `🎉 NEW user ${sanitizedNumber} successfully registered!`,
                            'success'
                        );
                    }

                } catch (e) {

                    arslanLog(
                        `Creds save error for ${sanitizedNumber}: ${e.message}`,
                        'error'
                    );
                }
            }
        );


        /* =================================================
         * ANTI DELETE
         * ================================================= */

        conn.ev.on(
            'messages.update',
            async updates => {

                try {

                    await handleAntidelete(
                        conn,
                        updates,
                        arslanStore
                    );

                } catch (e) {

                    arslanLog(
                        `Antidelete error: ${e.message}`,
                        'error'
                    );
                }
            }
        );


        /* =================================================
         * CONNECTION UPDATE
         * ================================================= */

        conn.ev.on(
            'connection.update',
            async update => {

                const {
                    connection,
                    lastDisconnect
                } = update;


                if (connection === 'open') {

                    try {

                        await arslanmd(conn);

                        arslanLog(
                            `Connected: ${sanitizedNumber}`,
                            'success'
                        );

                        const userJid =
                            jidNormalizedUser(
                                conn.user.id
                            );

                        await addNumberToMongoDB(
                            sanitizedNumber
                        );


                        if (!existingSession) {

                            try {

                                await conn.sendMessage(
                                    userJid,
                                    {
                                        image: {
                                            url:
                                                config.IMAGE_PATH
                                        },

                                        caption:
                                            `\n╭────────────────────◇\n│✦ *ARSLAN-MD — CONNECTED* 🔥\n│✦ Type *${prefix}menu* to see all commands 💫\n│✦ Prefix 『 ${prefix} 』  Mode 〔${mode}〕\n╰────────────────────○\n*© Powered by ✮⃝𝐀ⁿᵒⁿʸᵐᵒᵘˢ✮⃝ᵁˢᵉʳ ✮*`
                                    }
                                );

                            } catch (e) {

                                arslanLog(
                                    `Welcome message error: ${e.message}`,
                                    'warning'
                                );
                            }
                        }

                    } catch (e) {

                        arslanLog(
                            `Connection-open handler error: ${e.message}`,
                            'error'
                        );
                    }
                }


                if (
                    connection === 'close'
                ) {

                    const reason =
                        lastDisconnect?.error
                            ?.output?.statusCode ??
                        lastDisconnect?.error
                            ?.statusCode;

                    if (
                        reason ===
                        DisconnectReason.loggedOut
                    ) {
                        arslanLog(
                            `Session logged out for ${sanitizedNumber}.`,
                            'error'
                        );
                    }
                }
            }
        );


        /* =================================================
         * MESSAGE HANDLER
         * ================================================= */

        conn.ev.on(
            'messages.upsert',
            async msg => {

                try {

                    let mek =
                        msg.messages?.[0];

                    if (!mek) {
                        return;
                    }

                    if (!mek.message) {
                        return;
                    }


                    const userConfig =
                        await getUserConfigFromMongoDB(
                            sanitizedNumber
                        );


                    /* ==========================
                     * EPHEMERAL MESSAGE
                     * ========================== */

                    if (
                        getContentType(
                            mek.message
                        ) ===
                        'ephemeralMessage'
                    ) {
                        mek.message =
                            mek.message
                                .ephemeralMessage
                                .message;
                    }


                    /* ==========================
                     * READ MESSAGE
                     * ========================== */

                    if (
                        userConfig?.READ_MESSAGE ===
                        'true'
                    ) {
                        try {
                            await conn.readMessages([
                                mek.key
                            ]);
                        } catch (_) {}
                    }


                    /* ==========================
                     * NEWSLETTER REACTION
                     * ========================== */

                    const newsletterJids = [
                        '120363422524788798@newsletter'
                    ];

                    const newsEmojis = [
                        '❤️',
                        '👍',
                        '😮',
                        '😎',
                        '💀',
                        '💫',
                        '🔥',
                        '👑'
                    ];


                    if (
                        mek.key &&
                        newsletterJids.includes(
                            mek.key.remoteJid
                        )
                    ) {

                        try {

                            const serverId =
                                mek.newsletterServerId;

                            if (serverId) {

                                const emoji =
                                    newsEmojis[
                                        Math.floor(
                                            Math.random() *
                                            newsEmojis.length
                                        )
                                    ];

                                await conn.newsletterReactMessage(
                                    mek.key.remoteJid,
                                    serverId.toString(),
                                    emoji
                                );
                            }

                        } catch (_) {}
                    }


                    /* ==========================
                     * STATUS
                     * ========================== */

                    if (
                        mek.key &&
                        mek.key.remoteJid ===
                        'status@broadcast'
                    ) {

                        if (
                            userConfig?.AUTO_VIEW_STATUS ===
                            'true'
                        ) {
                            try {
                                await conn.readMessages([
                                    mek.key
                                ]);
                            } catch (_) {}
                        }


                        if (
                            userConfig?.AUTO_LIKE_STATUS ===
                            'true'
                        ) {

                            try {

                                const botJid =
                                    await conn.decodeJid(
                                        conn.user.id
                                    );

                                const emojis =
                                    userConfig.AUTO_LIKE_EMOJI ||
                                    config.AUTO_LIKE_EMOJI ||
                                    ['❤️'];

                                const emojiList =
                                    Array.isArray(emojis)
                                        ? emojis
                                        : String(emojis)
                                            .split('');

                                const randomEmoji =
                                    emojiList[
                                        Math.floor(
                                            Math.random() *
                                            emojiList.length
                                        )
                                    ];

                                await conn.sendMessage(
                                    mek.key.remoteJid,
                                    {
                                        react: {
                                            text:
                                                randomEmoji,
                                            key:
                                                mek.key
                                        }
                                    },
                                    {
                                        statusJidList: [
                                            mek.key.participant,
                                            botJid
                                        ]
                                    }
                                );

                            } catch (e) {

                                arslanLog(
                                    `Auto status like error: ${e.message}`,
                                    'debug'
                                );
                            }
                        }


                        if (
                            userConfig?.AUTO_STATUS_REPLY ===
                            'true'
                        ) {

                            try {

                                const user =
                                    mek.key.participant;

                                if (user) {

                                    await conn.sendMessage(
                                        user,
                                        {
                                            text:
                                                userConfig.AUTO_STATUS_MSG ||
                                                config.AUTO_STATUS_MSG ||
                                                'Status received.'
                                        },
                                        {
                                            quoted:
                                                mek
                                        }
                                    );
                                }

                            } catch (e) {

                                arslanLog(
                                    `Auto status reply error: ${e.message}`,
                                    'debug'
                                );
                            }
                        }

                        return;
                    }


                    /* ==========================
                     * MESSAGE PARSER
                     * ========================== */
                     const m =
                        sms(
                            conn,
                            mek
                        );

                    const type =
                        getContentType(
                            mek.message
                        );

                    const from =
                        mek.key.remoteJid;


                    const body =
                        type ===
                        'conversation'
                            ? mek.message.conversation

                            : type ===
                              'extendedTextMessage'
                                ? mek.message
                                    .extendedTextMessage
                                    .text

                                : type ===
                                  'imageMessage'
                                    ? mek.message
                                        .imageMessage
                                        .caption || ''

                                    : type ===
                                      'videoMessage'
                                        ? mek.message
                                            .videoMessage
                                            .caption || ''

                                        : '';


                    const isCmd =
                        body.startsWith(
                            config.PREFIX
                        );


                    const command =
                        isCmd
                            ? body
                                .slice(
                                    config.PREFIX.length
                                )
                                .trim()
                                .split(/\s+/)
                                .shift()
                                .toLowerCase()
                            : '';


                    const args =
                        body
                            .trim()
                            .split(/ +/)
                            .slice(1);


                    const q =
                        args.join(' ');

                    const text = q;


                    const isGroup =
                        from.endsWith(
                            '@g.us'
                        );


                    /* ==========================
                     * SENDER
                     * ========================== */

                    const sender =
                        mek.key.fromMe
                            ? (
                                conn.user.id
                                    .split(':')[0] +
                                '@s.whatsapp.net'
                            )
                            : (
                                mek.key.participant ||
                                mek.key.remoteJid
                            );


                    const senderNumber =
                        sender.split('@')[0];


                    const botNumber =
                        conn.user.id.split(':')[0];


                    const botNumber2 =
                        await jidNormalizedUser(
                            conn.user.id
                        );


                    const pushname =
                        mek.pushName ||
                        'User';


                    const isMe =
                        botNumber.includes(
                            senderNumber
                        );


                    const ownerNumbers =
                        Array.isArray(
                            config.OWNER_NUMBER
                        )
                            ? config.OWNER_NUMBER.map(
                                n => sanitizeNumber(n)
                            )
                            : String(
                                config.OWNER_NUMBER || ''
                            )
                                .split(',')
                                .map(n => sanitizeNumber(n))
                                .filter(Boolean);


                    const isOwner =
                        ownerNumbers.includes(
                            sanitizeNumber(
                                senderNumber
                            )
                        ) ||
                        isMe;


                    const isCreator =
                        isOwner;


                    /* ==========================
                     * GROUP DATA
                     * ========================== */

                    let groupMetadata = null;
                    let groupName = null;
                    let participants = null;

                    let groupAdmins = null;
                    let isBotAdmins = null;
                    let isAdmins = null;


                    if (isGroup) {

                        try {

                            groupMetadata =
                                await conn.groupMetadata(
                                    from
                                );

                            groupName =
                                groupMetadata.subject;

                            participants =
                                groupMetadata.participants;

                            groupAdmins =
                                getGroupAdmins(
                                    participants
                                );


                            isBotAdmins =
                                groupAdmins.includes(
                                    botNumber2
                                );


                            isAdmins =
                                groupAdmins.includes(
                                    sender
                                );

                        } catch (_) {}
                    }


                    /* ==========================
                     * PRESENCE
                     * ========================== */

                    if (
                        userConfig?.AUTO_TYPING ===
                        'true'
                    ) {

                        try {
                            await conn.sendPresenceUpdate(
                                'composing',
                                from
                            );
                        } catch (_) {}
                    }


                    if (
                        userConfig?.AUTO_RECORDING ===
                        'true'
                    ) {

                        try {
                            await conn.sendPresenceUpdate(
                                'recording',
                                from
                            );
                        } catch (_) {}
                    }


                    /* ==========================
                     * QUOTED MESSAGE
                     * ========================== */

                    const myquoted = {

                        key: {
                            remoteJid:
                                'status@broadcast',

                            participant:
                                '255794469700@s.whatsapp.net',

                            fromMe: false,

                            id:
                                createSerial(16)
                                    .toUpperCase()
                        },

                        message: {
                            contactMessage: {

                                displayName:
                                    '© ✮⃝𝐀ⁿᵒⁿʸᵐᵒᵘˢ✮⃝ᵁˢᵉʳ ✮',

                                vcard:
                                    `BEGIN:VCARD\nVERSION:3.0\nFN:✮⃝𝐀ⁿᵒⁿʸᵐᵒᵘˢ✮⃝ᵁˢᵉʳ ✮ BOY\nORG:✮⃝𝐀ⁿᵒⁿʸᵐᵒᵘˢ✮⃝ᵁˢᵉʳ ✮ BOY;\nTEL;type=CELL;type=VOICE;waid=255794469700:255634523742\nEND:VCARD`,

                                contextInfo: {
                                    stanzaId:
                                        createSerial(
                                            16
                                        ).toUpperCase(),

                                    participant:
                                        '0@s.whatsapp.net',

                                    quotedMessage: {
                                        conversation:
                                            '© ✮⃝𝐀ⁿᵒⁿʸᵐᵒᵘˢ✮⃝ᵁˢᵉʳ ✮'
                                    }
                                }
                            }
                        },

                        messageTimestamp:
                            Math.floor(
                                Date.now() / 1000
                            ),

                        status: 1,

                        verifiedBizName:
                            'Meta'
                    };


                    /* ==========================
                     * REPLY
                     * ========================== */

                    const reply = text =>
                        conn.sendMessage(
                            from,
                            {
                                text
                            },
                            {
                                quoted:
                                    myquoted
                            }
                        );

                    const l = reply;


                    /* =================================================
                     * COMMANDS
                     * ================================================= */

                    if (isCmd) {

                        try {

                            await incrementStats(
                                sanitizedNumber,
                                'commandsUsed'
                            );

                        } catch (_) {}


                        const cmd =
                            events.commands.find(
                                c =>
                                    c.pattern ===
                                    command
                            ) ||
                            events.commands.find(
                                c =>
                                    c.alias &&
                                    c.alias.includes(
                                        command
                                    )
                            );


                        if (cmd) {

                            if (
                                config.WORK_TYPE ===
                                    'private' &&
                                !isOwner
                            ) {
                                return;
                            }


                            if (cmd.react) {

                                try {

                                    await conn.sendMessage(
                                        from,
                                        {
                                            react: {
                                                text:
                                                    cmd.react,
                                                key:
                                                    mek.key
                                            }
                                        }
                                    );

                                } catch (_) {}
                            }


                            try {

                                await cmd.function(
                                    conn,
                                    mek,
                                    m,
                                    {
                                        from,
                                        quoted: mek,
                                        body,
                                        isCmd,
                                        command,
                                        args,
                                        q,
                                        text,
                                        isGroup,
                                        sender,
                                        senderNumber,
                                        botNumber2,
                                        botNumber,
                                        pushname,
                                        isMe,
                                        isOwner,
                                        isCreator,
                                        groupMetadata,
                                        groupName,
                                        participants,
                                        groupAdmins,
                                        isBotAdmins,
                                        isAdmins,
                                        reply,
                                        config,
                                        myquoted
                                    }
                                );

                            } catch (e) {

                                arslanLog(
                                    `PLUGIN ERROR [${command}]: ${e.stack || e.message}`,
                                    'error'
                                );
                            }
                        }
                    }


                    /* ==========================
                     * STATS
                     * ========================== */
                     

                    try {

                        await incrementStats(
                            sanitizedNumber,
                            'messagesReceived'
                        );

                        if (isGroup) {

                            await incrementStats(
                                sanitizedNumber,
                                'groupsInteracted'
                            );
                        }

                    } catch (_) {}


                    /* =================================================
                     * EVENT COMMANDS
                     * ================================================= */

                    for (
                        const evCmd
                        of events.commands
                    ) {

                        try {

                            const ctx = {
                                from,
                                l,
                                quoted: mek,
                                body,
                                isCmd,
                                command,
                                args,
                                q,
                                text,
                                isGroup,
                                sender,
                                senderNumber,
                                botNumber2,
                                botNumber,
                                pushname,
                                isMe,
                                isOwner,
                                isCreator,
                                groupMetadata,
                                groupName,
                                participants,
                                groupAdmins,
                                isBotAdmins,
                                isAdmins,
                                reply,
                                config,
                                myquoted
                            };


                            if (
                                body &&
                                evCmd.on ===
                                'body'
                            ) {

                                await evCmd.function(
                                    conn,
                                    mek,
                                    m,
                                    ctx
                                );

                            } else if (
                                mek.q &&
                                evCmd.on ===
                                'text'
                            ) {

                                await evCmd.function(
                                    conn,
                                    mek,
                                    m,
                                    ctx
                                );

                            } else if (
                                (
                                    evCmd.on ===
                                        'image' ||
                                    evCmd.on ===
                                        'photo'
                                ) &&
                                mek.type ===
                                    'imageMessage'
                            ) {

                                await evCmd.function(
                                    conn,
                                    mek,
                                    m,
                                    ctx
                                );

                            } else if (
                                evCmd.on ===
                                    'sticker' &&
                                mek.type ===
                                    'stickerMessage'
                            ) {

                                await evCmd.function(
                                    conn,
                                    mek,
                                    m,
                                    ctx
                                );
                            }

                        } catch (e) {

                            arslanLog(
                                `EVENT ERROR: ${e.stack || e.message}`,
                                'error'
                            );
                        }
                    }

                } catch (e) {

                    arslanLog(
                        `Message handler error: ${e.stack || e.message}`,
                        'error'
                    );
                }
            }
        );


    } catch (err) {

        arslanLog(
            `ARSLAN-MD Pair error: ${err.stack || err.message}`,
            'error'
        );

        if (
            res &&
            !res.headersSent
        ) {
            return res.status(500).json({
                error:
                    'Internal Server Error',

                details:
                    err.message
            });
        }

    } finally {

        if (connectionLockKey) {
            global[connectionLockKey] =
                false;
        }
    }
}


/* =========================================================
 * ROUTES
 * ========================================================= */

router.get(
    '/',
    (req, res) =>
        res.sendFile(
            path.join(
                __dirname,
                'pair.html'
            )
        )
);


router.get(
    '/code',
    async (req, res) => {

        if (!req.query.number) {
            return res.status(400).json({
                error:
                    'Number required'
            });
        }

        await arslanPair(
            req.query.number,
            res
        );
    }
);


router.get(
    '/status',
    async (req, res) => {

        const { number } =
            req.query;


        if (!number) {

            const list =
                Array.from(
                    activeSockets.keys()
                ).map(n => {

                    const s =
                        getConnectionStatus(n);

                    return {
                        number: n,

                        status:
                            'connected',

                        connectionTime:
                            s.connectionTime,

                        uptime:
                            `${s.uptime} seconds`
                    };
                });


            return res.json({
                totalActive:
                    activeSockets.size,

                connections:
                    list
            });
        }


        const s =
            getConnectionStatus(
                number
            );


        res.json({

            number:
                sanitizeNumber(number),

            isConnected:
                s.isConnected,

            connectionTime:
                s.connectionTime,

            uptime:
                `${s.uptime} seconds`
        });
    }
);


/* =========================================================
 * DISCONNECT
 * ========================================================= */

router.get(
    '/disconnect',
    async (req, res) => {

        const { number } =
            req.query;


        if (!number) {
            return res.status(400).json({
                error:
                    'Number required'
            });
        }


        const n =
            sanitizeNumber(number);


        if (!activeSockets.has(n)) {
            return res.status(404).json({
                error:
                    'Not found'
            });
        }


        try {

            const socket =
                activeSockets.get(n);


            try {
                await socket.ws.close();
            } catch (_) {}


            try {
                socket.ev.removeAllListeners();
            } catch (_) {}


            activeSockets.delete(n);

            socketCreationTime.delete(n);

            pairingCodes.delete(n);

            pairingInProgress.delete(n);


            try {
                await deleteSessionFromMongoDB(n);
            } catch (e) {
                arslanLog(
                    `Delete Mongo session error: ${e.message}`,
                    'warning'
                );
            }


            try {
                await removeNumberFromMongoDB(n);
            } catch (e) {
                arslanLog(
                    `Remove Mongo number error: ${e.message}`,
                    'warning'
                );
            }


            res.json({
                status:
                    'success',

                message:
                    'Disconnected'
            });

        } catch (e) {

            arslanLog(
                `Disconnect error for ${n}: ${e.message}`,
                'error'
            );

            res.status(500).json({
                error:
                    'Failed to disconnect',

                message:
                    e.message
            });
        }
    }
);


/* =========================================================
 * ACTIVE
 * ========================================================= */

router.get(
    '/active',
    (req, res) =>
        res.json({
            count:
                activeSockets.size,

            numbers:
                Array.from(
                    activeSockets.keys()
                )
        })
);


/* =========================================================
 * PING
 * ========================================================= */

router.get(
    '/ping',
    (req, res) =>
        res.json({
            status:
                'active',

            message:
                'Arslan-md is running 🔥',

            activeSessions:
                activeSockets.size
        })
);


/* =========================================================
 * CONNECT ALL
 * ========================================================= */

router.get(
    '/connect-all',
    async (req, res) => {

        try {

            const numbers =
                await getAllNumbersFromMongoDB();


            if (!numbers.length) {
                return res.status(404).json({
                    error:
                        'No numbers found'
                });
            }


            const results = [];


            for (
                const number
                of numbers
            ) {

                const n =
                    sanitizeNumber(number);


                if (
                    activeSockets.has(n)
                ) {

                    results.push({
                        number: n,

                        status:
                            'already_connected'
                    });

                    continue;
                }


                const mockRes = {

                    headersSent:
                        false,

                    json: () => {},

                    send: () => {},

                    status() {
                        return mockRes;
                    }
                };


                await arslanPair(
                    n,
                    mockRes
                );


                results.push({
                    number: n,

                    status:
                        'connection_initiated'
                });


                await delay(1000);
            }


            res.json({

                status:
                    'success',

                total:
                    numbers.length,

                connections:
                    results
            });

        } catch (e) {

            arslanLog(
                `Connect-all error: ${e.message}`,
                'error'
            );

            res.status(500).json({
                error:
                    'Failed',

                message:
                    e.message
            });
        }
    }
);


/* =========================================================
 * UPDATE CONFIG
 * ========================================================= */

router.get(
    '/update-config',
    async (req, res) => {

        const {
            number,
            config: configString
        } = req.query;


        if (
            !number ||
            !configString
        ) {
            return res.status(400).json({
                error:
                    'Number and config required'
            });
        }


        let newConfig;


        try {

            newConfig =
                JSON.parse(
                    configString
                );

        } catch (_) {

            return res.status(400).json({
                error:
                    'Invalid config'
            });
        }


        const n =
            sanitizeNumber(number);


        const socket =
            activeSockets.get(n);


        if (!socket) {
            return res.status(404).json({
                error:
                    'No active session'
            });
        }


        const otp =
            Math.floor(
                100000 +
                Math.random() *
                900000
            ).toString();


        try {

            await saveOTPToMongoDB(
                n,
                otp,
                newConfig
            );


            await socket.sendMessage(
                jidNormalizedUser(
                    socket.user.id
                ),
                {
                    text:
                        `*🔐 ARSLAN-MD — CONFIG UPDATE*\n\nOTP: *${otp}*\nValid 5 minutes`
                }
            );


            res.json({
                status:
                    'otp_sent'
            });

        } catch (e) {

            arslanLog(
                `Config OTP error: ${e.message}`,
                'error'
            );

            res.status(500).json({
                error:
                    'Failed to send OTP',

                message:
                    e.message
            });
        }
    }
);


/* =========================================================
 * VERIFY OTP
 * ========================================================= */

router.get(
    '/verify-otp',
    async (req, res) => {

        const {
            number,
            otp
        } = req.query;


        if (
            !number ||
            !otp
        ) {
            return res.status(400).json({
                error:
                    'Number and OTP required'
            });
        }


        try {

            const n =
                sanitizeNumber(number);


            const verification =
                await verifyOTPFromMongoDB(
                    n,
                    otp
                );


            if (!verification.valid) {
                return res.status(400).json({
                    error:
                        verification.error
                });
            }


            await updateUserConfigInMongoDB(
                n,
                verification.config
            );


            const socket =
                activeSockets.get(n);


            if (socket) {

                try {

                    await socket.sendMessage(
                        jidNormalizedUser(
                            socket.user.id
                        ),
                        {
                            text:
                                '*✅ CONFIG UPDATED*'
                        }
                    );

                } catch (_) {}
            }


            res.json({
                status:
                    'success'
            });

        } catch (e) {

            arslanLog(
                `OTP verification error: ${e.message}`,
                'error'
            );

            res.status(500).json({
                error:
                    'Failed to verify OTP',

                message:
                    e.message
            });
        }
    }
);


/* =========================================================
 * STATS
 * ========================================================= */

router.get(
    '/stats',
    async (req, res) => {

        const { number } =
            req.query;


        if (!number) {
            return res.status(400).json({
                error:
                    'Number required'
            });
        }


        try {

            const n =
                sanitizeNumber(number);


            const stats =
                await getStatsForNumber(n);


            const s =
                getConnectionStatus(n);


            res.json({

                number: n,

                connectionStatus:
                    s.isConnected
                        ? 'Connected'
                        : 'Disconnected',

                uptime:
                    s.uptime,

                stats
            });

        } catch (e) {

            arslanLog(
                `Stats error: ${e.message}`,
                'error'
            );

            res.status(500).json({
                error:
                    'Failed',

                message:
                    e.message
            });
        }
    }
);


/* =========================================================
 * AUTO RECONNECT
 * ========================================================= */
 