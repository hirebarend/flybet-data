const https = require("https");
const http = require("http");

const FLIGHT_INFO_URL =
  "https://www.airports.co.za/utilities/live-flight-info";
const FLIGHT_INFO_POST_URL =
  "https://www.airports.co.za/utilities/live-flight-info?TermStoreId=5bf3754c-f4cc-4939-a24c-690f3cd01918&TermSetId=f37980a4-c1c3-44da-98fe-118b3c090a62&TermId=0daa09a9-7873-434a-84ae-f417369895c4";
const DEFAULT_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  Accept:
    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
  "Accept-Language": "en-US,en;q=0.5",
};

/**
 * Makes an HTTPS GET or POST request and returns the response body as a string.
 * Uses Node.js built-in https/http modules (no external dependencies).
 */
function httpRequest(url, options = {}) {
  return new Promise((resolve, reject) => {
    const parsedUrl = new URL(url);
    const transport = parsedUrl.protocol === "https:" ? https : http;

    const reqOptions = {
      hostname: parsedUrl.hostname,
      port: parsedUrl.port || (parsedUrl.protocol === "https:" ? 443 : 80),
      path: parsedUrl.pathname + parsedUrl.search,
      method: options.method || "GET",
      headers: options.headers || {},
    };

    const req = transport.request(reqOptions, (res) => {
      const chunks = [];
      res.on("data", (chunk) => chunks.push(chunk));
      res.on("end", () => {
        const body = Buffer.concat(chunks).toString("utf-8");
        resolve({
          statusCode: res.statusCode,
          headers: res.headers,
          body,
        });
      });
    });

    req.on("error", (err) => reject(err));

    if (options.timeout) {
      req.setTimeout(options.timeout, () => {
        req.destroy(new Error("Request timed out"));
      });
    }

    if (options.body) {
      req.write(options.body);
    }

    req.end();
  });
}

/**
 * Extracts cookies from response headers and formats them for the Cookie header.
 */
function extractCookies(response) {
  const setCookieHeaders = response.headers["set-cookie"];
  if (!setCookieHeaders) return "";
  return setCookieHeaders
    .map((cookie) => cookie.split(";")[0])
    .join("; ");
}

/**
 * Extracts a hidden form field value from the HTML.
 */
function extractFormField(html, fieldName) {
  const pattern = new RegExp(
    `id="${fieldName}"[^>]*value="([^"]*)"`,
    "i"
  );
  const match = html.match(pattern);
  return match ? match[1] : "";
}

/**
 * Calculates tomorrow's date in YYYY-MM-DD format, using South African time (UTC+2).
 * South Africa does not observe daylight saving time, so UTC+2 is always correct.
 */
function getTomorrowDateSAST() {
  const saTime = new Date(Date.now() + 2 * 3600000);
  saTime.setDate(saTime.getDate() + 1);
  return saTime.toISOString().split("T")[0];
}

/**
 * Parses flight card HTML elements into structured objects.
 */
function parseFlightCards(html) {
  const flights = [];
  const cardRegex =
    /<div class="flight-card">([\s\S]*?)<\/div>\s*<\/div>\s*<\/div>/g;

  let match;
  while ((match = cardRegex.exec(html)) !== null) {
    const card = match[1];

    const flightNumber = extractText(card, "flight-number");
    const airline = extractText(card, "flight-airline");
    const statusBadge = extractStatusBadge(card);
    const details = extractDetails(card);

    flights.push({
      flightNumber,
      airline,
      status: statusBadge,
      scheduleTime: details["Schedule Time"] || null,
      departingFrom: details["Departing From"] || null,
      destination: details["Destination"] || null,
      flightStatus: details["Flight Status"] || null,
      updatedTime: details["Updated Time"] || null,
      checkInCounters: details["Check-in Counters"] || null,
    });
  }

  return flights;
}

/**
 * Extracts text content from an element by its CSS class.
 */
function extractText(html, className) {
  const regex = new RegExp(
    `class="${className}"[^>]*>([\\s\\S]*?)<\\/div>`,
    "i"
  );
  const match = html.match(regex);
  return match ? match[1].trim() : null;
}

/**
 * Extracts the status badge text from a flight card.
 */
function extractStatusBadge(html) {
  const regex = /class="status-badge[^"]*"[^>]*>([\s\S]*?)<\/div>/i;
  const match = html.match(regex);
  return match ? match[1].trim() : null;
}

/**
 * Extracts all detail label/value pairs from a flight card.
 */
function extractDetails(html) {
  const details = {};
  const labelRegex = /class="detail-label"[^>]*>([\s\S]*?)<\/div>/gi;
  const valueRegex = /class="detail-value"[^>]*>([\s\S]*?)<\/div>/gi;

  const labels = [];
  const values = [];

  let m;
  while ((m = labelRegex.exec(html)) !== null) {
    labels.push(m[1].trim());
  }
  while ((m = valueRegex.exec(html)) !== null) {
    values.push(m[1].trim());
  }

  for (let i = 0; i < labels.length; i++) {
    if (i < values.length) {
      details[labels[i]] = values[i];
    }
  }

  return details;
}

/**
 * Fetches the ACSA flight search page and extracts the form tokens
 * (__VIEWSTATE, __EVENTVALIDATION) needed for the search POST request.
 *
 * @returns {Promise<{ viewState: string, eventValidation: string, cookies: string }>}
 */
async function fetchFormTokens() {
  const response = await httpRequest(FLIGHT_INFO_URL, {
    timeout: 30000,
    headers: { ...DEFAULT_HEADERS },
  });

  if (response.statusCode !== 200) {
    throw new Error(
      `Failed to load flight info page: HTTP ${response.statusCode}`
    );
  }

  const viewState = extractFormField(response.body, "__VIEWSTATE");
  const eventValidation = extractFormField(
    response.body,
    "__EVENTVALIDATION"
  );

  if (!viewState || !eventValidation) {
    throw new Error("Failed to extract form tokens from the page");
  }

  const cookies = extractCookies(response);

  return { viewState, eventValidation, cookies };
}

/**
 * Builds the URL-encoded form body for the flight search POST request.
 */
function buildSearchFormBody(viewState, eventValidation, dateFrom, dateTo) {
  const prefix =
    "ctl00$ctl49$g_3d254c0d_a345_4dc8_b11b_9b55f87a5892$ctl00$";

  const params = new URLSearchParams();
  params.append("__VIEWSTATE", viewState);
  params.append("__VIEWSTATEGENERATOR", "13236C49");
  params.append("__EVENTVALIDATION", eventValidation);
  params.append("__EVENTTARGET", prefix + "btnSearchHidden");
  params.append("__EVENTARGUMENT", "");
  params.append(prefix + "hdnSearchMode", "advanced");
  params.append(prefix + "hdnFlightNumber", "");
  params.append(prefix + "hdnArrivalDeparture", "both");
  params.append(prefix + "hdnFromAirport", "");
  params.append(prefix + "hdnToAirport", "");
  params.append(prefix + "hdnFlightType", "");
  params.append(prefix + "hdnAirline", "");
  params.append(prefix + "hdnDateFrom", dateFrom);
  params.append(prefix + "hdnDateTo", dateTo);
  params.append(prefix + "hdnTimeFrom", "00:00");
  params.append(prefix + "hdnTimeTo", "23:59");

  return params.toString();
}

/**
 * Fetches all flights (arrivals and departures) for the following day
 * from the South African Airports Company (ACSA) website.
 *
 * This function:
 * 1. Loads the flight info page to obtain ASP.NET form tokens
 * 2. Submits a search for all flights on the next day (using South African time)
 * 3. Parses the HTML response and returns structured flight data
 *
 * @returns {Promise<Array<{
 *   flightNumber: string,
 *   airline: string,
 *   status: string,
 *   scheduleTime: string,
 *   departingFrom: string,
 *   destination: string,
 *   flightStatus: string,
 *   updatedTime: string | null,
 *   checkInCounters: string | null
 * }>>}
 */
async function getFlightsForTomorrow() {
  const tomorrow = getTomorrowDateSAST();

  const { viewState, eventValidation, cookies } = await fetchFormTokens();

  const formBody = buildSearchFormBody(
    viewState,
    eventValidation,
    tomorrow,
    tomorrow
  );

  const headers = {
    ...DEFAULT_HEADERS,
    "Content-Type": "application/x-www-form-urlencoded",
    "Content-Length": Buffer.byteLength(formBody).toString(),
    Referer: FLIGHT_INFO_URL,
    Origin: "https://www.airports.co.za",
  };

  if (cookies) {
    headers["Cookie"] = cookies;
  }

  const response = await httpRequest(FLIGHT_INFO_POST_URL, {
    method: "POST",
    headers,
    body: formBody,
    timeout: 90000,
  });

  if (response.statusCode !== 200) {
    throw new Error(
      `Flight search request failed: HTTP ${response.statusCode}`
    );
  }

  return parseFlightCards(response.body);
}

module.exports = {
  getFlightsForTomorrow,
  getTomorrowDateSAST,
  parseFlightCards,
};

// Allow running directly: node src/main.js
if (require.main === module) {
  getFlightsForTomorrow()
    .then((flights) => {
      console.log(JSON.stringify(flights, null, 2));
      console.log(`\nTotal flights: ${flights.length}`);
    })
    .catch((err) => {
      console.error("Error:", err.message);
      process.exit(1);
    });
}
