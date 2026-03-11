import { sleep } from "@blowater/csp";
import { newURL, parseJSON, RESTRequestFailed } from "./_helper.ts";
import { prepareNostrEvent } from "./event.ts";
import { PublicKey } from "./key.ts";
import { getRelayInformation, type RelayInformation } from "./nip11.ts";
import { NoteID } from "./nip19.ts";
import {
    type _RelayResponse,
    type ClientRequest_REQ,
    type NostrEvent,
    type NostrFilter,
    NostrKind,
    type RelayResponse_REQ_Message,
    type Signer,
} from "./nostr.ts";
import type { Closer, EventSender, Subscriber, SubscriptionCloser } from "./relay.interface.ts";
import {
    AsyncWebSocket,
    CloseTwice,
    type WebSocketClosedEvent,
    type WebSocketError,
    type WebSocketReadyState,
} from "./websocket.ts";
import * as csp from "@blowater/csp";
import { getSpaceMembers, prepareSpaceMember } from "./space-member.ts";
import { assertEquals } from "@std/assert";
import type { Event_V2, Signer_V2, SpaceMember } from "./v2.ts";
import { Negentropy, type NegentropyItem } from "./negentropy.ts";

export class WebSocketClosed extends Error {
    constructor(
        public url: string | URL,
        public state: WebSocketReadyState,
        public reason?: WebSocketClosedEvent,
    ) {
        super(`${url} is in state ${state}, code ${reason?.code}`);
        this.name = WebSocketClosed.name;
    }
}

export class RelayDisconnectedByClient extends Error {
    constructor() {
        super();
        this.name = RelayDisconnectedByClient.name;
    }
}

export class FailedToLookupAddress extends Error {}

export type NextMessageType = {
    type: "messsage";
    data: string;
} | {
    type: "WebSocketClosed";
    error: WebSocketClosed;
} | {
    type: "RelayDisconnectedByClient";
    error: RelayDisconnectedByClient;
} | {
    type: "FailedToLookupAddress";
    error: string;
} | {
    type: "OtherError";
    error: WebSocketError;
} | {
    type: "open";
} | {
    type: "closed";
    event: WebSocketClosedEvent;
};

export type BidirectionalNetwork = {
    status(): WebSocketReadyState;
    untilOpen(): Promise<WebSocketClosed | undefined>;
    nextMessage(): Promise<
        NextMessageType
    >;
    send: (
        str: string | ArrayBufferLike | Blob | ArrayBufferView,
    ) => Promise<WebSocketClosed | Error | undefined>;
    close: (
        code?: number,
        reason?: string,
        force?: boolean,
    ) => Promise<CloseTwice | WebSocketClosedEvent | undefined>;
};

export class SubscriptionAlreadyExist extends Error {
    constructor(public subID: string, public url: string) {
        super(`subscription '${subID}' already exists for ${url}`);
    }
}

export type SubscriptionStream = {
    filters: NostrFilter[];
    chan: csp.Channel<RelayResponse_REQ_Message>;
};

export type NegentropySync = {
    have: string[];
    need: string[];
};

/**
 * [examples](./tests/example.test.ts)
 */
export class SingleRelayConnection implements Subscriber, SubscriptionCloser, EventSender, Closer {
    private _isClosedByClient = false;
    isClosedByClient() {
        return this._isClosedByClient;
    }

    private subscriptionMap = new Map<
        string,
        SubscriptionStream
    >();
    readonly send_promise_resolvers = new Map<
        string,
        (res: { ok: boolean; message: string }) => void
    >();
    private error: AuthError | RelayDisconnectedByClient | undefined; // todo: check this error in public APIs
    private ws: BidirectionalNetwork | undefined;

    status(): WebSocketReadyState {
        if (this.ws == undefined) {
            return "Closed";
        }
        return this.ws.status();
    }

    private constructor(
        readonly url: URL,
        readonly wsCreator: (url: string, log: boolean) => BidirectionalNetwork | Error,
        public log: boolean,
        readonly signer?: Signer,
        readonly signer_v2?: Signer_V2,
    ) {
        (async () => {
            const ws = await this.connect();
            if (ws instanceof Error) {
                this.error = ws;
                return ws;
            }
            this.ws = ws;
            for (;;) {
                const messsage = await this.nextMessage(this.ws);
                if (messsage.type == "RelayDisconnectedByClient") {
                    this.error = messsage.error;
                    // exit the coroutine
                    return messsage.error;
                } else if (
                    messsage.type == "WebSocketClosed" ||
                    messsage.type == "FailedToLookupAddress" ||
                    messsage.type == "OtherError" || messsage.type == "closed"
                ) {
                    if (messsage.type != "closed") {
                        if (messsage.error instanceof Error) {
                            this.error = messsage.error;
                        } else if (typeof messsage.error == "string") {
                            this.error = new Error(messsage.error);
                        } else {
                            console.error(messsage);
                            this.error = new Error(messsage.error.error);
                        }
                    }
                    if (messsage.type == "closed") {
                        // https://www.rfc-editor.org/rfc/rfc6455.html#section-7.4
                        // https://www.iana.org/assignments/websocket/websocket.xml#close-code-number
                        if (messsage.event.code == 3000) {
                            // close all sub channels
                            for (const stream of this.subscriptionMap) {
                                const e = await this.closeSub(stream[0]);
                                if (e instanceof Error) {
                                    console.error(e);
                                }
                            }
                            const err = new AuthError(messsage.event.reason);
                            // resolve all send_promise_resolvers to false
                            for (const [_, resolver] of this.send_promise_resolvers) {
                                resolver({
                                    ok: false,
                                    message: err.message,
                                });
                            }
                            return err;
                        }
                    }
                    if (this._isClosedByClient == false) {
                        console.log("connection error", messsage);
                        const err = await this.connect();
                        if (err instanceof RelayDisconnectedByClient) {
                            return err;
                        }
                        if (err instanceof Error) {
                            console.error(err);
                            this.error = err;
                        }
                    }
                    continue;
                } else if (messsage.type == "open") {
                    if (this.log) {
                        console.log(`relay connection ${this.url} is openned`);
                    }
                    // the websocket is just openned
                    // send all the subscriptions to the relay
                    for (const [subID, data] of this.subscriptionMap.entries()) {
                        if (this.ws == undefined) {
                            console.error("impossible state");
                            break;
                        }
                        const err = await sendSubscription(this.ws, subID, ...data.filters);
                        if (err instanceof Error) {
                            console.error(err);
                        }
                    }
                } else {
                    const relayResponse = parseJSON<_RelayResponse>(messsage.data);
                    if (relayResponse instanceof Error) {
                        console.error(relayResponse);
                        continue;
                    }

                    if (
                        relayResponse[0] === "EVENT" ||
                        relayResponse[0] === "EOSE"
                    ) {
                        const subID = relayResponse[1];
                        const stream = this.subscriptionMap.get(subID);
                        if (stream == undefined) {
                            // This can happen when the subscription is closed
                            // but the relay sends a message before it receives the CLOSE
                            continue;
                        }
                        if (relayResponse[0] === "EVENT") {
                            const err = await stream.chan.put({
                                type: "EVENT",
                                subID,
                                event: relayResponse[2],
                            });
                            if (err instanceof csp.PutToClosedChannelError) {
                                console.error(err);
                            }
                        } else {
                            const err = await stream.chan.put({
                                type: "EOSE",
                                subID,
                            });
                            if (err instanceof csp.PutToClosedChannelError) {
                                console.error(err);
                            }
                        }
                    } else if (relayResponse[0] === "NOTICE") {
                        for (const stream of this.subscriptionMap.values()) {
                            const err = await stream.chan.put({
                                type: "NOTICE",
                                note: relayResponse[1],
                            });
                            if (err instanceof csp.PutToClosedChannelError) {
                                console.error(err);
                            }
                        }
                    } else if (relayResponse[0] === "OK") {
                        const resolver = this.send_promise_resolvers.get(relayResponse[1]);
                        if (resolver) {
                            resolver({ ok: relayResponse[2], message: relayResponse[3] });
                            this.send_promise_resolvers.delete(relayResponse[1]);
                        }
                    } else if (relayResponse[0] === "AUTH") {
                        if (this.signer == undefined) {
                            continue;
                        }
                        const challenge = relayResponse[1];
                        if (this.ws == undefined) {
                            console.error("impossible state");
                            continue;
                        }
                        const event = await prepareNostrEvent(this.signer, {
                            kind: NostrKind.HTTP_AUTH,
                            content: "",
                            tags: [
                                ["relay", this.url.toString()],
                                ["challenge", challenge],
                            ],
                        });
                        if (event instanceof Error) {
                            console.error(event);
                            continue;
                        }
                        const err = await this.ws.send(JSON.stringify(["AUTH", event]));
                        if (err instanceof Error) {
                            console.error(err);
                        }
                    }
                }
            }
        })();
    }

    static New(
        url: string | URL,
        args?: {
            wsCreator?: (url: string, log: boolean) => BidirectionalNetwork | Error;
            log?: boolean;
            signer?: Signer;
            signer_v2?: Signer_V2;
        },
    ) {
        let theURL: URL;
        if (typeof url == "string") {
            const _url = newURL(url);
            if (_url instanceof TypeError) {
                return _url;
            }
            theURL = _url;
        } else {
            theURL = url;
        }
        return new SingleRelayConnection(
            theURL,
            args?.wsCreator || AsyncWebSocket.New,
            args?.log || false,
            args?.signer,
            args?.signer_v2,
        );
    }

    async newSub(
        subID: string,
        ...filters: NostrFilter[]
    ): Promise<SubscriptionStream | SubscriptionAlreadyExist | WebSocketClosed> {
        if (this.subscriptionMap.has(subID)) {
            return new SubscriptionAlreadyExist(subID, this.url.toString());
        }
        const c = csp.chan<RelayResponse_REQ_Message>();
        this.subscriptionMap.set(subID, { filters, chan: c });
        if (this.ws != undefined && this.ws.status() == "Open") {
            const err = await sendSubscription(this.ws, subID, ...filters);
            if (err instanceof Error) {
                return err;
            }
        }
        return { filters, chan: c };
    }

    async closeSub(subID: string) {
        const stream = this.subscriptionMap.get(subID);
        if (stream == undefined) {
            return;
        }
        this.subscriptionMap.delete(subID);
        const err = stream.chan.close();
        if (err instanceof csp.CloseChannelError) {
            console.error(err);
        }
        if (this.ws == undefined) {
            return;
        }
        const sendErr = await this.ws.send(JSON.stringify(["CLOSE", subID]));
        if (sendErr instanceof Error) {
            return sendErr;
        }
    }

    async sendEvent(nostrEvent: NostrEvent) {
        if (this.ws == undefined) {
            return new WebSocketClosed(this.url, "Closed");
        }
        if (this.ws.status() != "Open") {
            return new WebSocketClosed(this.url, this.ws.status());
        }
        return new Promise<{ ok: boolean; message: string }>((resolve) => {
            this.send_promise_resolvers.set(nostrEvent.id, resolve);
            if (this.ws == undefined) {
                resolve({ ok: false, message: "ws is undefined" });
                return;
            }
            this.ws.send(JSON.stringify(["EVENT", nostrEvent])).then((err) => {
                if (err instanceof Error) {
                    resolve({ ok: false, message: err.message });
                }
            });
        });
    }

    isClosed() {
        return this._isClosedByClient || this.ws?.status() == "Closed";
    }

    async close() {
        this._isClosedByClient = true;
        if (this.ws == undefined) {
            return;
        }
        // close all sub channels
        for (const [subID, _] of this.subscriptionMap.entries()) {
            const stream = this.subscriptionMap.get(subID);
            if (stream == undefined) {
                continue;
            }
            this.subscriptionMap.delete(subID);
            stream.chan.close();
        }
        const err = await this.ws.close();
        if (err instanceof CloseTwice) {
            // don't care
        } else if (err instanceof Error) {
            console.error(err);
        }
    }

    async getEvent(id: NoteID | string): Promise<NostrEvent | undefined | Error> {
        if (id instanceof NoteID) {
            id = id.hex;
        }
        const stream = await this.newSub(id, { "ids": [id] });
        if (stream instanceof Error) {
            return stream;
        }
        for await (const msg of stream.chan) {
            if (msg.type == "EOSE") {
                await this.closeSub(id);
                return undefined;
            } else if (msg.type == "EVENT") {
                await this.closeSub(id);
                return msg.event;
            }
        }
    }

    async getRelayInformation(): Promise<RelayInformation | RESTRequestFailed | Error> {
        const httpURL = new URL(this.url.toString());
        if (httpURL.protocol == "wss:") {
            httpURL.protocol = "https:";
        } else {
            httpURL.protocol = "http:";
        }
        return getRelayInformation(httpURL.toString());
    }

    async getSpaceMembers(publicKey: PublicKey): Promise<SpaceMember[] | Error> {
        return getSpaceMembers(this, publicKey);
    }

    /**
     * Synchronize events with the relay using the Negentropy protocol.
     * Returns the list of event IDs the client needs from the relay (need)
     * and the list of event IDs the relay needs from the client (have).
     *
     * @param filter - The subscription filter defining the range to sync
     * @param localItems - The local events the client already has
     */
    async negentropySync(
        subID: string,
        filter: NostrFilter,
        localItems: NegentropyItem[],
    ): Promise<NegentropySync | Error> {
        if (this.ws == undefined || this.ws.status() != "Open") {
            return new WebSocketClosed(this.url, this.ws?.status() ?? "Closed");
        }

        const neg = new Negentropy();
        for (const item of localItems) {
            neg.addItem(item.created_at, item.id);
        }
        neg.seal();

        const initialMsg = neg.initiate();

        // Send NEG-OPEN
        const openMsg = JSON.stringify(["NEG-OPEN", subID, filter, initialMsg]);
        const sendErr = await this.ws.send(openMsg);
        if (sendErr instanceof Error) {
            return sendErr;
        }

        const have: string[] = [];
        const need: string[] = [];

        // Process NEG-MSG responses until done
        for (;;) {
            const message = await this.nextMessage(this.ws);
            if (message.type !== "messsage") {
                return new Error(`unexpected message type during negentropy sync: ${message.type}`);
            }

            const parsed = parseJSON<unknown[]>(message.data);
            if (parsed instanceof Error) {
                return parsed;
            }

            if (!Array.isArray(parsed) || parsed.length < 2) {
                continue;
            }

            const msgType = parsed[0];
            const msgSubID = parsed[1];

            if (msgSubID !== subID) {
                continue;
            }

            if (msgType === "NEG-ERR") {
                const closeErr = await this.ws.send(JSON.stringify(["NEG-CLOSE", subID]));
                if (closeErr instanceof Error) {
                    console.error(closeErr);
                }
                return new Error(`NEG-ERR from relay: ${parsed[2]}`);
            }

            if (msgType === "NEG-HAVE") {
                // Relay has these IDs that the client doesn't have
                const ids = parsed[2] as string[];
                for (const id of ids) {
                    need.push(id);
                }
                continue;
            }

            if (msgType === "NEG-NEED") {
                // Client has these IDs that the relay doesn't have
                const ids = parsed[2] as string[];
                for (const id of ids) {
                    have.push(id);
                }
                continue;
            }

            if (msgType === "NEG-MSG") {
                const replyMsg = parsed[2] as string;
                const result = neg.reconcile(replyMsg);

                for (const id of result.have) {
                    have.push(id);
                }
                for (const id of result.need) {
                    need.push(id);
                }

                if (result.output === null) {
                    // Sync is complete
                    const closeErr = await this.ws.send(JSON.stringify(["NEG-CLOSE", subID]));
                    if (closeErr instanceof Error) {
                        console.error(closeErr);
                    }
                    return { have, need };
                }

                // Send next round
                const nextMsg = JSON.stringify(["NEG-MSG", subID, result.output]);
                const err = await this.ws.send(nextMsg);
                if (err instanceof Error) {
                    return err;
                }
                continue;
            }
        }
    }

    private async connect(): Promise<BidirectionalNetwork | RelayDisconnectedByClient | Error> {
        if (this._isClosedByClient) {
            return new RelayDisconnectedByClient();
        }
        const ws = this.wsCreator(this.url.toString(), this.log);
        if (ws instanceof Error) {
            return ws;
        }
        this.ws = ws;
        const err = await ws.untilOpen();
        if (err instanceof WebSocketClosed) {
            return err;
        }
        return ws;
    }

    private async nextMessage(ws: BidirectionalNetwork): Promise<NextMessageType> {
        return ws.nextMessage();
    }
}

async function sendSubscription(
    ws: BidirectionalNetwork,
    subID: string,
    ...filters: NostrFilter[]
) {
    const req: ClientRequest_REQ = ["REQ", subID, ...filters];
    const err = await ws.send(JSON.stringify(req));
    if (err instanceof Error) {
        return err;
    }
}

export class AuthError extends Error {
    constructor(reason: string) {
        super(reason);
        this.name = AuthError.name;
    }
}
