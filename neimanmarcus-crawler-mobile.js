/** @format */

/**
 * Neiman Marcus Mobile Crawler for Android Termux
 * Optimized for mobile IPs and user agents to bypass 403 errors
 *
 * INSTALLATION:
 * 1. Install Termux from F-Droid
 * 2. Run: pkg update && pkg upgrade
 * 3. Run: pkg install nodejs git
 * 4. Copy this file to Termux: ~/crawler/neimanmarcus-crawler-mobile.js
 * 5. Run: npm install axios https-proxy-agent
 * 6. Run: node neimanmarcus-crawler-mobile.js
 */

const fs = require("fs");
const path = require("path");

// Try to load helper, but work without it if not available
let retryRequestWithProxyRotation, createAxiosInstance;
try {
  const helper = require("./utils/helper");
  retryRequestWithProxyRotation = helper.retryRequestWithProxyRotation;
  createAxiosInstance = helper.createAxiosInstance;
} catch (e) {
  console.log("⚠️ Helper.js not found, using basic axios");
  const axios = require("axios");
  const { HttpsProxyAgent } = require("https-proxy-agent");

  createAxiosInstance = (proxy = null) => {
    const config = {
      timeout: 60000,
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
        Accept: "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
      },
    };
    if (proxy?.endpoint) {
      config.httpsAgent = new HttpsProxyAgent(proxy.endpoint);
      config.httpAgent = new HttpsProxyAgent(proxy.endpoint);
    }
    return axios.create(config);
  };

  retryRequestWithProxyRotation = async (
    requestFunc,
    maxRetries = 3,
    baseDelay = 2000
  ) => {
    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        const axiosInstance = createAxiosInstance();
        return await requestFunc(axiosInstance);
      } catch (error) {
        if (attempt === maxRetries - 1) throw error;
        await new Promise((r) => setTimeout(r, baseDelay * (attempt + 1)));
      }
    }
  };
}

// Mobile-optimized headers
const HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
  Accept: "application/json, text/plain, */*",
  "Accept-Language": "en-US,en;q=0.9",
  "Accept-Encoding": "gzip, deflate, br",
  Referer: "https://www.neimanmarcus.com/",
  Origin: "https://www.neimanmarcus.com",
  Connection: "keep-alive",
  "Sec-Fetch-Dest": "empty",
  "Sec-Fetch-Mode": "cors",
  "Sec-Fetch-Site": "same-origin",
  "Cache-Control": "no-cache",
  Pragma: "no-cache",
};

let isBlocked = false;

// Copy all functions from original crawler (abbreviated for space)
// In practice, copy the full functions from neimanmarcus-crawler.js

function isBlockedResponse(response, data) {
  if (!response) return false;
  if (response.status === 403) return true;
  if (data) {
    const dataStr =
      typeof data === "string" ? data : JSON.stringify(data).toLowerCase();
    return [
      "captcha",
      "challenge",
      "access denied",
      "blocked",
      "datadome",
    ].some((indicator) => dataStr.includes(indicator));
  }
  return false;
}

function generateUUIDv5(name, namespace) {
  const str = namespace + name;
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = (hash << 5) - hash + str.charCodeAt(i);
    hash = hash & hash;
  }
  const hex = Math.abs(hash).toString(16).padStart(8, "0");
  return `${hex.slice(0, 8)}-${hex.slice(0, 4)}-5${hex.slice(1, 4)}-${hex.slice(
    0,
    4
  )}-${hex.slice(0, 12)}`;
}

function generateImageVariants(baseImageUrl, msid, colorCode) {
  const variants = [];
  if (!baseImageUrl) return variants;
  const msidMatch = baseImageUrl.match(/nm_(\d+)_(\d+)_[mabcz]/i);
  if (msidMatch) {
    const baseUrl = `https://media.neimanmarcus.com/f_auto,q_auto:low,ar_4:5,c_fill,dpr_2.0,w_790/01/nm_${msidMatch[1]}_${msidMatch[2]}_`;
    ["m", "a", "b", "c", "z"].forEach((suffix) =>
      variants.push(`${baseUrl}${suffix}`)
    );
  } else {
    variants.push(baseImageUrl);
  }
  return variants;
}

function extractProductDataFromNextData(html) {
  try {
    const match = html.match(
      /<script[^>]*id=["']__NEXT_DATA__["'][^>]*>([\s\S]*?)<\/script>/i
    );
    if (match) {
      const nextData = JSON.parse(match[1]);
      return nextData?.props?.pageProps?.productData || null;
    }
  } catch (e) {
    console.log(`⚠️ Error extracting __NEXT_DATA__: ${e.message}`);
  }
  return null;
}

// Copy extractFromProductData function from original (too long to include here)
// For now, use simplified version or copy full function

async function fetchProductDetails(product) {
  const msid = product.msid || product.id;
  const canonical = product.canonical || product.url || "";
  const url = canonical.startsWith("http")
    ? canonical
    : `https://www.neimanmarcus.com${canonical}`;

  console.log(`  🔍 Fetching: ${msid} - ${url}`);

  try {
    const response = await retryRequestWithProxyRotation(
      async (axiosInstance) => {
        const res = await axiosInstance.get(url, { headers: HEADERS });
        if (isBlockedResponse(res, res.data)) {
          isBlocked = true;
          throw new Error("403 Forbidden detected");
        }
        return res;
      },
      2,
      3000
    );

    if (response?.data) {
      const productData = extractProductDataFromNextData(response.data);
      if (productData) {
        // Process productData (simplified - copy full logic from original)
        return {
          parent_product_id: productData.id || "",
          name: productData.name || "",
          description: productData.details?.longDesc || "",
          variants:
            productData.skus?.map((sku) => ({
              size: sku.size?.name || "",
              color: sku.color?.name || "",
              is_in_stock: sku.inStock || false,
            })) || [],
        };
      }
    }
  } catch (error) {
    if (error.response?.status === 403 || error.message?.includes("403")) {
      isBlocked = true;
      throw error;
    }
    console.log(`  ❌ Fetch failed: ${error.message}`);
  }
  return null;
}

async function fetchAllProducts() {
  const all = [];
  let page = 1;

  while (!isBlocked) {
    const listUrl = `https://www.neimanmarcus.com/c/dt/api/productlisting?categoryId=cat58290731&page=${page}&parentCategoryId=&navPath=`;

    try {
      const response = await retryRequestWithProxyRotation(
        async (axiosInstance) => {
          const res = await axiosInstance.get(listUrl, { headers: HEADERS });
          if (isBlockedResponse(res, res.data)) {
            isBlocked = true;
            throw new Error("403 Forbidden detected");
          }
          return res;
        },
        2,
        3000
      );

      const products = response?.data?.products || [];
      if (products.length === 0) break;

      console.log(`✅ Fetched ${products.length} products from page ${page}`);
      all.push(...products);
      page++;

      await new Promise((r) => setTimeout(r, 2000)); // Longer delay for mobile
    } catch (error) {
      if (error.response?.status === 403) {
        isBlocked = true;
        break;
      }
      console.log(`❌ Error page ${page}: ${error.message}`);
      break;
    }
  }

  return all;
}

function saveJSONFile(data, filename) {
  const outputDir = path.join(process.cwd(), "output", "US", "neimanmarcus-US");
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }
  const filePath = path.join(outputDir, filename);
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2), "utf8");
  console.log(`💾 Saved: ${filePath}`);
  return filePath;
}

async function main() {
  console.log("🚀 Neiman Marcus Mobile Crawler (Termux)");
  console.log("📱 Using mobile user agent to bypass 403\n");

  try {
    const allProducts = await fetchAllProducts();

    if (isBlocked) {
      console.log("\n❌ Blocked - try switching network or VPN");
      process.exit(1);
    }

    console.log(`\n📊 Processing ${allProducts.length} products...\n`);

    const CHUNK_SIZE = 100; // Smaller chunks for mobile
    const results = [];

    for (let i = 0; i < Math.min(allProducts.length, 50); i++) {
      // Limit for testing
      if (isBlocked) break;

      const product = allProducts[i];
      try {
        const details = await fetchProductDetails(product);
        if (details) {
          results.push(details);
          console.log(
            `✔ [${i + 1}/${Math.min(allProducts.length, 50)}] ${details.name}`
          );
        }
        await new Promise((r) => setTimeout(r, 1000)); // 1 second delay
      } catch (err) {
        if (err.message?.includes("403")) {
          isBlocked = true;
          break;
        }
        console.log(`⚠️ [${i + 1}] Failed: ${err.message}`);
      }
    }

    if (results.length > 0) {
      const timestamp = new Date()
        .toISOString()
        .replace(/[:.]/g, "-")
        .slice(0, -5);
      saveJSONFile(results, `catalog-mobile-${timestamp}.json`);
      console.log(`\n✅ Saved ${results.length} products`);
    }
  } catch (error) {
    console.error("\n❌ Error:", error.message);
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}

module.exports = { main, fetchAllProducts, fetchProductDetails };
