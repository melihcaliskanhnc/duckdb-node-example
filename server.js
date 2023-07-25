#! /usr/bin/env node
import { DuckDB } from "./DuckDB.js";
import { dataServer } from "./data-server.js";
const dbPath = ":memory:";

dataServer(new DuckDB(dbPath), { rest: true, socket: true });
