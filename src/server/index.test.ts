import { describe, it, expect } from "bun:test";
import { app } from "./index";

async function post(path: string, body: unknown) {
    const req = new Request(`http://localhost${path}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
    });
    return app.fetch(req);
}

async function get(path: string) {
    const req = new Request(`http://localhost${path}`);
    return app.fetch(req);
}

describe("server handler", () => {
    it("inserts rows and returns meta; GET returns sorted rows", async () => {
        const postRes = await post("/t1/p1/users", [
            { $time: 2, name: "bob" },
            { $time: 1, name: "alice" },
        ]);
        expect(postRes.ok).toBeTrue();
        const postJson = (await postRes.json()) as any;
        expect(postJson.ok).toBeTrue();
        expect(postJson.inserted).toBe(2);
        expect(postJson.meta.order.key).toBe("$time");

        const getRes = await get("/t1/p1/users");
        expect(getRes.ok).toBeTrue();
        const getJson = (await getRes.json()) as any;
        expect(getJson.ok).toBeTrue();
        expect(getJson.rows.map((r: any) => r.$time)).toEqual([1, 2]);
        expect(getJson.rows.map((r: any) => r.name)).toEqual(["alice", "bob"]);
    });

    it("errors when a row is missing the order key", async () => {
        const res = await post("/t2/p2/events", [
            { $time: 1, event: "a" },
            { event: "b" },
        ] as any);
        expect(res.status).toBe(400);
        const json = (await res.json()) as any;
        expect(json.ok).toBeFalse();
        expect(String(json.error)).toContain("missing required order key");
    });

    it("errors when order key type mismatches config", async () => {
        const res = await post("/t4/p4/mismatch", [
            { $time: "2", name: "bad" },
        ] as any);
        expect(res.status).toBe(400);
        const json = (await res.json()) as any;
        expect(json.ok).toBeFalse();
        expect(String(json.error)).toContain("type mismatch");
    });

    it("errors on empty rows", async () => {
        const res = await post("/t3/p3/empty", []);
        expect(res.status).toBe(400);
        const json = (await res.json()) as any;
        expect(json.ok).toBeFalse();
    });
});


