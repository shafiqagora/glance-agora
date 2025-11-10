/**
 * Marshalls API Discovery Script
 * Intercepts network requests to find the product listing API
 *
 * @format
 */

const puppeteer = require("puppeteer-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");
const fs = require("fs");
const path = require("path");

// Add stealth plugin to avoid detection
puppeteer.use(StealthPlugin());

async function discoverMarshallsAPI() {
  console.log("🚀 Starting Marshalls API Discovery...\n");

  const browser = await puppeteer.launch({
    headless: false, // Set to false to see what's happening
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
      "--disable-blink-features=AutomationControlled", // Remove automation flag
    ],
  });

  const page = await browser.newPage();

  // Remove webdriver property
  await page.evaluateOnNewDocument(() => {
    Object.defineProperty(navigator, "webdriver", {
      get: () => false,
    });
  });

  // Set realistic user agent and headers
  await page.setUserAgent(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
  );

  // Set extra headers
  await page.setExtraHTTPHeaders({
    Accept:
      "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    Connection: "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
  });

  // Set viewport
  await page.setViewport({ width: 1920, height: 1080 });

  // Arrays to store intercepted requests/responses
  const interceptedRequests = [];
  const interceptedResponses = [];
  const productAPIs = [];

  // Enable request interception
  await page.setRequestInterception(true);

  // Intercept requests
  page.on("request", (request) => {
    const url = request.url();
    const method = request.method();
    const headers = request.headers();
    const postData = request.postData();

    // Log all requests (filter for API-like calls)
    if (
      url.includes("/api/") ||
      url.includes("/graphql") ||
      url.includes("/rest/") ||
      url.includes("/v1/") ||
      url.includes("/v2/") ||
      url.includes("product") ||
      url.includes("search") ||
      url.includes("catalog") ||
      url.includes("listing") ||
      url.includes("plp") ||
      url.endsWith(".json") ||
      headers["content-type"]?.includes("application/json")
    ) {
      interceptedRequests.push({
        url,
        method,
        headers,
        postData,
        resourceType: request.resourceType(),
      });

      console.log(`📤 [${method}] ${url}`);
    }

    request.continue();
  });

  // Intercept responses
  page.on("response", async (response) => {
    const url = response.url();
    const status = response.status();
    const headers = response.headers();
    const contentType = headers["content-type"] || "";

    // Check if it's a JSON response or API-like URL
    if (
      (contentType.includes("application/json") ||
        url.includes("/api/") ||
        url.includes("/rest/") ||
        url.includes("/v1/") ||
        url.includes("/v2/") ||
        url.includes("product") ||
        url.includes("search") ||
        url.includes("catalog") ||
        url.includes("listing") ||
        url.includes("plp")) &&
      status === 200
    ) {
      try {
        const responseBody = await response.text();
        let jsonData = null;

        try {
          jsonData = JSON.parse(responseBody);
        } catch (e) {
          // Not JSON, skip
        }

        interceptedResponses.push({
          url,
          status,
          headers,
          contentType,
          bodySize: responseBody.length,
          hasJson: !!jsonData,
        });

        // Check if response contains product data
        if (jsonData) {
          const bodyStr = JSON.stringify(jsonData).toLowerCase();
          if (
            bodyStr.includes("product") ||
            bodyStr.includes("item") ||
            bodyStr.includes("sku") ||
            bodyStr.includes("price") ||
            bodyStr.includes("image") ||
            bodyStr.includes("name") ||
            bodyStr.includes("title")
          ) {
            productAPIs.push({
              url,
              method: response.request().method(),
              status,
              sampleData: jsonData,
              dataSize: responseBody.length,
            });

            console.log(`\n✅ FOUND PRODUCT API: ${url}`);
            console.log(`   Status: ${status}`);
            console.log(
              `   Data Size: ${(responseBody.length / 1024).toFixed(2)} KB`
            );
            console.log(`   Sample Keys:`, Object.keys(jsonData).slice(0, 10));
          }
        }
      } catch (error) {
        // Ignore errors
      }
    }
  });

  // Helper function to wait (replacement for deprecated waitForTimeout)
  const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

  // Navigate to the product listing page
  const homepageUrl = "https://www.marshalls.com";
  const targetUrl =
    "https://www.marshalls.com/us/store/shop/womens-clothing/_/N-3255077828?mm=Women%3Af%3A+%3A17%3AView+All%3AClothing";

  // First, visit homepage to establish session and avoid direct bot detection
  console.log(`\n🌐 Step 1: Visiting homepage to establish session...`);
  try {
    await page.goto(homepageUrl, {
      waitUntil: "domcontentloaded",
      timeout: 30000,
    });
    await wait(2000); // Wait a bit like a real user
  } catch (error) {
    console.log("⚠️  Homepage visit had issues, continuing anyway...");
  }

  // Now navigate to the product listing page
  console.log(`\n🌐 Step 2: Navigating to product listing page...`);
  console.log(`   ${targetUrl}\n`);

  try {
    await page.goto(targetUrl, {
      waitUntil: "domcontentloaded", // Changed from networkidle2 to avoid timeout
      timeout: 60000,
    });

    // Wait a bit more for any lazy-loaded API calls
    console.log("\n⏳ Waiting for API calls to complete...");
    await wait(5000);

    // Try scrolling to trigger lazy loading
    console.log("📜 Scrolling to trigger lazy loading...");
    await page.evaluate(() => {
      window.scrollTo(0, document.body.scrollHeight / 2);
    });
    await wait(2000);

    await page.evaluate(() => {
      window.scrollTo(0, document.body.scrollHeight);
    });
    await wait(3000);
  } catch (error) {
    console.error("❌ Error navigating:", error.message);
  }

  // Summary
  console.log("\n" + "=".repeat(80));
  console.log("📊 DISCOVERY SUMMARY");
  console.log("=".repeat(80));
  console.log(`\n📤 Total API-like requests: ${interceptedRequests.length}`);
  console.log(`📥 Total API-like responses: ${interceptedResponses.length}`);
  console.log(`✅ Product APIs found: ${productAPIs.length}\n`);

  if (productAPIs.length > 0) {
    console.log("\n🎯 PRODUCT API ENDPOINTS:");
    console.log("-".repeat(80));
    productAPIs.forEach((api, index) => {
      console.log(`\n${index + 1}. ${api.method} ${api.url}`);
      console.log(`   Status: ${api.status}`);
      console.log(`   Data Size: ${(api.dataSize / 1024).toFixed(2)} KB`);
      if (api.sampleData) {
        console.log(
          `   Sample Structure:`,
          JSON.stringify(api.sampleData, null, 2).substring(0, 500)
        );
      }
    });
  }

  // Save results to file
  const outputDir = path.join(__dirname, "output");
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }

  const results = {
    timestamp: new Date().toISOString(),
    targetUrl,
    summary: {
      totalRequests: interceptedRequests.length,
      totalResponses: interceptedResponses.length,
      productAPIsFound: productAPIs.length,
    },
    productAPIs,
    allRequests: interceptedRequests.slice(0, 50), // Limit to first 50
    allResponses: interceptedResponses.slice(0, 50), // Limit to first 50
  };

  const outputFile = path.join(outputDir, "marshalls-api-discovery.json");
  fs.writeFileSync(outputFile, JSON.stringify(results, null, 2));
  console.log(`\n💾 Results saved to: ${outputFile}`);

  // Keep browser open for inspection (comment out if you want it to close automatically)
  console.log(
    "\n⏸️  Keeping browser open for 30 seconds for manual inspection..."
  );
  console.log("   (Close manually or wait for auto-close)");
  await wait(30000);

  await browser.close();
  console.log("\n✅ Discovery complete!");
}

// Run the discovery
discoverMarshallsAPI().catch((error) => {
  console.error("❌ Error:", error);
  process.exit(1);
});
