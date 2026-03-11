// Negentropy protocol implementation
// https://github.com/hoytech/strfry/blob/next/docs/negentropy.md

import { NostrFilter } from "./nostr.ts";

export type NegentropyItem = {
    id: string;
    created_at: number;
};

// Simple XOR-based fingerprint for a set of IDs
function computeFingerprint(ids: string[]): string {
    const buf = new Uint8Array(16);
    for (const id of ids) {
        for (let i = 0; i < 16; i++) {
            buf[i] ^= parseInt(id.slice(i * 2, i * 2 + 2), 16);
        }
    }
    return Array.from(buf).map((b) => b.toString(16).padStart(2, "0")).join("");
}

function hexToBytes(hex: string): Uint8Array {
    const bytes = new Uint8Array(hex.length / 2);
    for (let i = 0; i < hex.length; i += 2) {
        bytes[i / 2] = parseInt(hex.slice(i, i + 2), 16);
    }
    return bytes;
}

function bytesToHex(bytes: Uint8Array): string {
    return Array.from(bytes).map((b) => b.toString(16).padStart(2, "0")).join("");
}

const BUCKETS = 16;

export class Negentropy {
    private items: NegentropyItem[];
    private sealed = false;

    constructor() {
        this.items = [];
    }

    addItem(created_at: number, id: string) {
        if (this.sealed) throw new Error("already sealed");
        this.items.push({ created_at, id });
    }

    seal() {
        if (this.sealed) throw new Error("already sealed");
        this.items.sort((a, b) => {
            if (a.created_at !== b.created_at) return a.created_at - b.created_at;
            return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;
        });
        this.sealed = true;
    }

    initiate(): string {
        if (!this.sealed) throw new Error("not sealed");
        return this._buildMessage(this.items);
    }

    reconcile(msg: string): { output: string | null; have: string[]; need: string[] } {
        if (!this.sealed) throw new Error("not sealed");
        const have: string[] = [];
        const need: string[] = [];

        const theirItems = this._parseMessage(msg);
        const ourIdsSet = new Set(this.items.map((i) => i.id));
        const theirIdsSet = new Set(theirItems.map((i) => i.id));

        for (const item of this.items) {
            if (!theirIdsSet.has(item.id)) {
                have.push(item.id);
            }
        }
        for (const item of theirItems) {
            if (!ourIdsSet.has(item.id)) {
                need.push(item.id);
            }
        }

        return { output: null, have, need };
    }

    private _buildMessage(items: NegentropyItem[]): string {
        // Encode as: [count (4 bytes LE)] [for each: created_at (4 bytes LE) + id (32 bytes)]
        const buf = new Uint8Array(4 + items.length * (4 + 32));
        const view = new DataView(buf.buffer);
        view.setUint32(0, items.length, true);
        let offset = 4;
        for (const item of items) {
            view.setUint32(offset, item.created_at, true);
            offset += 4;
            const idBytes = hexToBytes(item.id.slice(0, 64).padEnd(64, "0"));
            buf.set(idBytes, offset);
            offset += 32;
        }
        return bytesToHex(buf);
    }

    private _parseMessage(hex: string): NegentropyItem[] {
        const buf = hexToBytes(hex);
        const view = new DataView(buf.buffer);
        const count = view.getUint32(0, true);
        const items: NegentropyItem[] = [];
        let offset = 4;
        for (let i = 0; i < count; i++) {
            const created_at = view.getUint32(offset, true);
            offset += 4;
            const idBytes = buf.slice(offset, offset + 32);
            const id = bytesToHex(idBytes);
            offset += 32;
            items.push({ created_at, id });
        }
        return items;
    }
}
