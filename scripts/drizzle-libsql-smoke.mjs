#!/usr/bin/env node

/*
 * Smoke test for Drizzle ORM using its libSQL driver against Orion.
 *
 * Required:
 *   LIBSQL_URL or first positional argument
 *     Defaults to http://127.0.0.1:8091/drizzle_smoke for local runs.
 *
 * Optional:
 *   LIBSQL_AUTH_TOKEN
 */

import { createClient } from "@libsql/client";
import { eq, sql } from "drizzle-orm";
import { drizzle } from "drizzle-orm/libsql";
import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core";

const DEFAULT_URL = "http://127.0.0.1:8091/drizzle_smoke";

const url = process.argv[2] ?? process.env.LIBSQL_URL ?? DEFAULT_URL;
const authToken = process.env.LIBSQL_AUTH_TOKEN;
const tableName = `orion_drizzle_smoke_${process.pid}_${Date.now().toString(36)}`;
const accounts = sqliteTable(tableName, {
  id: integer("id").primaryKey(),
  email: text("email").notNull().unique(),
  name: text("name").notNull(),
  visits: integer("visits").notNull().default(0),
});
const checks = [];

const client = createClient({ url, authToken });
const db = drizzle(client);

try {
  await pass("create table", async () => {
    await db.run(sql.raw([
      `create table ${quoteIdentifier(tableName)} (`,
      "  id integer primary key,",
      "  email text not null unique,",
      "  name text not null,",
      "  visits integer not null default 0",
      ")",
    ].join(" ")));
  });

  await pass("insert and select", async () => {
    await db.insert(accounts).values([
      { id: 1, email: "ada@example.com", name: "Ada", visits: 2 },
      { id: 2, email: "grace@example.com", name: "Grace", visits: 4 },
    ]);

    const rows = await db
      .select({
        id: accounts.id,
        email: accounts.email,
        visits: accounts.visits,
      })
      .from(accounts)
      .where(eq(accounts.email, "ada@example.com"));
    assertEqual(rows, [{ id: 1, email: "ada@example.com", visits: 2 }], "selected row");
  });

  await pass("update returning", async () => {
    const rows = await db
      .update(accounts)
      .set({ visits: 5 })
      .where(eq(accounts.id, 1))
      .returning({ email: accounts.email, visits: accounts.visits });
    assertEqual(rows, [{ email: "ada@example.com", visits: 5 }], "updated row");
  });

  await pass("transaction commit", async () => {
    await db.transaction(async (tx) => {
      await tx.insert(accounts).values({
        id: 3,
        email: "margaret@example.com",
        name: "Margaret",
        visits: 1,
      });
      await tx
        .update(accounts)
        .set({ visits: 6 })
        .where(eq(accounts.email, "margaret@example.com"));
    });
    const rows = await db
      .select({ visits: accounts.visits })
      .from(accounts)
      .where(eq(accounts.email, "margaret@example.com"));
    assertEqual(rows, [{ visits: 6 }], "transaction result");
  });

  await pass("constraint error", async () => {
    let error;
    try {
      await db.insert(accounts).values({
        id: 4,
        email: "ada@example.com",
        name: "Duplicate",
        visits: 0,
      });
    } catch (caught) {
      error = caught;
    }
    if (!error) {
      throw new Error("duplicate unique value did not fail");
    }
    if (!String(error.code ?? error.cause?.code ?? error.message).includes("CONSTRAINT")) {
      throw new Error(`expected constraint error, received ${formatError(error)}`);
    }
  });

  console.log(JSON.stringify({ ok: true, url, checks }, null, 2));
} catch (error) {
  console.error(JSON.stringify({
    ok: false,
    url,
    checks,
    error: describeError(error),
  }, null, 2));
  process.exitCode = 1;
} finally {
  await client.execute(`drop table if exists ${quoteIdentifier(tableName)}`).catch(() => {});
  client.close();
}

async function pass(name, fn) {
  await fn();
  checks.push(name);
}

function quoteIdentifier(value) {
  return `"${value.replaceAll('"', '""')}"`;
}

function assertEqual(actual, expected, label) {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`${label}: expected ${JSON.stringify(expected)}, received ${JSON.stringify(actual)}`);
  }
}

function formatError(error) {
  return JSON.stringify({
    name: error?.name,
    message: error?.message,
    code: error?.code,
    cause: error?.cause
      ? {
          name: error.cause.name,
          message: error.cause.message,
          code: error.cause.code,
        }
      : undefined,
  });
}

function describeError(error) {
  return {
    name: error?.name,
    message: error?.message,
    code: error?.code,
    cause: error?.cause
      ? {
          name: error.cause.name,
          message: error.cause.message,
          code: error.cause.code,
        }
      : undefined,
  };
}
