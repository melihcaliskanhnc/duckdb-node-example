import http from "node:http";
import url from "node:url";

export function dataServer(db, { rest = true, port = 3000 } = {}) {
  const queryCache = null;
  const handleQuery = queryHandler(db, queryCache);
  const app = createHTTPServer(handleQuery, rest);

  app.listen(port);
  console.log(`Data server running on port ${port}`);
  if (rest) console.log(`  http://localhost:${port}/`);
}

function createHTTPServer(handleQuery, rest) {
  return http.createServer((req, resp) => {
    const res = httpResponse(resp);
    if (!rest) {
      res.done();
      return;
    }

    resp.setHeader("Access-Control-Allow-Origin", "*");
    resp.setHeader("Access-Control-Request-Method", "*");
    resp.setHeader("Access-Control-Allow-Methods", "OPTIONS, POST, GET");
    resp.setHeader("Access-Control-Allow-Headers", "*");
    resp.setHeader("Access-Control-Max-Age", 2592000);

    switch (req.method) {
      case "OPTIONS":
        res.done();
        break;
      case "GET":
        handleQuery(res, url.parse(req.url, true).query);
        break;
      case "POST": {
        const chunks = [];
        req.on("error", (err) => res.error(err, 500));
        req.on("data", (chunk) => chunks.push(chunk));
        req.on("end", () => handleQuery(res, Buffer.concat(chunks)));
        break;
      }
      default:
        res.error(`Unsupported HTTP method: ${req.method}`, 400);
    }
  });
}

function queryHandler(db) {
  async function retrieve(query, get) {
    const { sql, type } = query;
    result = await get(sql);
    return result;
  }

  // query request handler
  return async (res, data) => {
    const t0 = performance.now();

    // parse incoming query
    let query;
    try {
      query = JSON.parse(data);
    } catch (err) {
      res.error(err, 400);
      return;
    }

    try {
      const { sql, type = "json" } = query;
      console.log(`> ${type.toUpperCase()}${sql ? " " + sql : ""}`);

      // process query and return result
      switch (type) {
        case "exec":
          // Execute query with no return value
          await db.exec(sql);
          res.done();
          break;
        case "arrow":
          // Apache Arrow response format
          res.arrow(await retrieve(query, (sql) => db.arrowBuffer(sql)));
          break;
        case "json":
          // JSON response format
          res.json(await retrieve(query, (sql) => db.query(sql)));
          break;

        default:
          res.error(`Unrecognized command: ${type}`, 400);
      }
    } catch (err) {
      res.error(err, 500);
    }

    console.log("REQUEST", (performance.now() - t0).toFixed(1));
  };
}

function httpResponse(res) {
  return {
    arrow(data) {
      res.setHeader("Content-Type", "application/vnd.apache.arrow.stream");
      res.end(data);
    },
    json(data) {
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify(data));
    },
    done() {
      res.writeHead(200);
      res.end();
    },
    error(err, code) {
      console.error(err);
      res.writeHead(code);
      res.end();
    },
  };
}
