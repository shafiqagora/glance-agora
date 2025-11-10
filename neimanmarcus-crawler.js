/** @format */

/**
 * Neiman Marcus Node.js Crawler with Comprehensive Data Extraction
 *
 * MASTER DATA SOURCE: __NEXT_DATA__ (productData object in HTML)
 * This contains ALL product details: description, materials, SKUs, colors, sizes, images, availability, stock levels
 *
 * Features:
 * - Uses proxy rotation from helper.js
 * - Strong error handling for 403/CAPTCHA errors
 * - Processes products in chunks of 500
 * - Saves results to JSON files
 */

const fs = require("fs");
const path = require("path");
const {
  retryRequestWithProxyRotation,
  createAxiosInstance,
} = require("./utils/helper");

// Headers for Neiman Marcus requests
const HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "x-datadome-clientid":
    "muExzPsBYAQ3HukTtR6ReYxLiyrQEvtRqplh8i7uGpwLCkNeBQNH9tDo8wjvXDxb74UzaCOFVxnN~oNRoTUA3B678ZphKL9qjy_NX4UF_SeL8yGXtUh8Kt0tTJzwoRKn",
  Cookie: "WID=4bf375e1-570c-4770-9c79-269a6f82b66a; PLP_ONLY_X_LEFT=true;",
  Accept: "application/json, text/plain, */*",
  "Accept-Language": "en-US,en;q=0.9",
  Referer: "https://www.neimanmarcus.com/",
};

// Flag to track if we've encountered 403/CAPTCHA
let isBlocked = false;

/**
 * Custom retry function that stops immediately on 403 errors
 * Unlike the helper's retryRequestWithProxyRotation, this stops on 403
 */
async function retryRequestStopOn403(
  requestFunc,
  maxRetries = 5,
  baseDelay = 2000,
  country = "US"
) {
  let attempt = 0;

  // Import proxy rotator logic from helper
  // We need to recreate the proxy config since proxyRotator isn't exported
  const proxyConfig = {
    provider: "decodo",
    credentials: {
      username: process.env.PROXY_USERNAME || "splmzpsd06",
      password: process.env.PROXY_PASSWORD || "es7s2W=dDbn6rGy4En",
      endpoint:
        country === "US"
          ? "us.decodo.com"
          : country === "IN"
          ? "in.decodo.com"
          : "us.decodo.com",
      port: process.env.PROXY_PORT || 10000,
      isRotating: true,
    },
  };

  const getProxyForCountry = (country) => {
    const endpoint =
      country === "US"
        ? "us.decodo.com"
        : country === "IN"
        ? "in.decodo.com"
        : "us.decodo.com";
    return {
      endpoint: `https://${proxyConfig.credentials.username}:${proxyConfig.credentials.password}@${endpoint}:${proxyConfig.credentials.port}`,
      type: "decodo-rotating",
      port: proxyConfig.credentials.port,
      country: country,
    };
  };

  while (attempt < maxRetries) {
    try {
      // Get proxy for country
      const currentProxy = getProxyForCountry(country);
      const axiosInstance = createAxiosInstance(currentProxy);
      const response = await requestFunc(axiosInstance);

      // Check for blocking immediately
      if (response && isBlockedResponse(response, response.data)) {
        isBlocked = true;
        const error = new Error("403 Forbidden or CAPTCHA detected");
        error.response = response;
        throw error;
      }

      return response;
    } catch (error) {
      attempt++;

      // Stop immediately on 403 errors - don't retry
      if (
        error.response?.status === 403 ||
        error.message?.includes("403") ||
        error.message?.includes("CAPTCHA") ||
        error.message?.includes("Forbidden")
      ) {
        isBlocked = true;
        console.log(`❌ BLOCKED: 403/CAPTCHA detected - stopping immediately`);
        throw error;
      }

      // Handle 429 errors with exponential backoff
      if (error.response?.status === 429) {
        const retryAfter = error.response.headers["retry-after"];
        const delay = retryAfter
          ? Math.min(parseInt(retryAfter) * 1000, 30000)
          : Math.min(baseDelay * Math.pow(2, attempt), 30000);

        console.log(
          `Rate limited (429) - waiting ${delay}ms. Attempt ${attempt}/${maxRetries}`
        );

        if (attempt < maxRetries) {
          await new Promise((resolve) => setTimeout(resolve, delay));
          continue;
        }
      }

      // Handle timeout errors
      if (error.code === "ECONNABORTED" && error.message.includes("timeout")) {
        const delay = Math.min(baseDelay * Math.pow(2, attempt), 15000);
        console.log(
          `Request timeout - waiting ${delay}ms. Attempt ${attempt}/${maxRetries}`
        );

        if (attempt < maxRetries) {
          await new Promise((resolve) => setTimeout(resolve, delay));
          continue;
        }
      }

      // Handle connection errors
      if (
        error.code === "ECONNRESET" ||
        error.code === "ETIMEDOUT" ||
        error.code === "ENOTFOUND" ||
        error.response?.status >= 500
      ) {
        const delay = Math.min(baseDelay * attempt, 5000);
        console.log(
          `Connection error: ${
            error.code || error.response?.status
          }. Attempt ${attempt}/${maxRetries}`
        );

        if (attempt < maxRetries) {
          await new Promise((resolve) => setTimeout(resolve, delay));
          continue;
        }
      }

      // For other errors, retry with delay
      if (error.response?.status && error.response.status < 500) {
        const delay = Math.min(baseDelay * attempt, 3000);
        console.log(
          `HTTP error ${error.response.status} - retrying. Attempt ${attempt}/${maxRetries}`
        );

        if (attempt < maxRetries) {
          await new Promise((resolve) => setTimeout(resolve, delay));
          continue;
        }
      }

      if (attempt === maxRetries) {
        throw error;
      }
    }
  }
}

/**
 * Check if response indicates 403 or CAPTCHA blocking
 */
function isBlockedResponse(response, data) {
  if (!response) return false;

  // Check for 403 status
  if (response.status === 403) {
    return true;
  }

  // Check response body for CAPTCHA indicators
  if (data) {
    const dataStr =
      typeof data === "string" ? data : JSON.stringify(data).toLowerCase();
    const captchaIndicators = [
      "captcha",
      "challenge",
      "access denied",
      "blocked",
      "datadome",
      "cloudflare",
      "verify you are human",
      "unusual traffic",
    ];

    return captchaIndicators.some((indicator) => dataStr.includes(indicator));
  }

  return false;
}

/**
 * Extract and generate all image variants from a base image URL
 */
function generateImageVariants(baseImageUrl, msid, colorCode) {
  const variants = [];

  if (!baseImageUrl) return variants;

  // Extract msid and colorCode from URL pattern: nm_5013376_100189_m
  const msidMatch = baseImageUrl.match(/nm_(\d+)_(\d+)_[mabcz]/i);
  if (msidMatch) {
    const extractedMsid = msidMatch[1];
    const extractedColorCode = msidMatch[2];

    // Construct base URL with correct format
    const baseUrl = `https://media.neimanmarcus.com/f_auto,q_auto:low,ar_4:5,c_fill,dpr_2.0,w_790/01/nm_${extractedMsid}_${extractedColorCode}_`;
    const suffixes = ["m", "a", "b", "c", "z"];

    for (const suffix of suffixes) {
      variants.push(`${baseUrl}${suffix}`);
    }
  } else if (msid && colorCode) {
    // If we have msid and colorCode but no base URL pattern, construct from scratch
    const baseUrl = `https://media.neimanmarcus.com/f_auto,q_auto:low,ar_4:5,c_fill,dpr_2.0,w_790/01/nm_${msid}_${colorCode}_`;
    const suffixes = ["m", "a", "b", "c", "z"];

    for (const suffix of suffixes) {
      variants.push(`${baseUrl}${suffix}`);
    }
  } else {
    // Fallback: use the provided URL as-is (single image)
    variants.push(baseImageUrl);
  }

  return variants;
}

/**
 * Generate UUID v5 hash (simple implementation)
 */
function generateUUIDv5(name, namespace) {
  const str = namespace + name;
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = (hash << 5) - hash + char;
    hash = hash & hash; // Convert to 32bit integer
  }
  // Convert to UUID-like format
  const hex = Math.abs(hash).toString(16).padStart(8, "0");
  return `${hex.slice(0, 8)}-${hex.slice(0, 4)}-5${hex.slice(1, 4)}-${hex.slice(
    0,
    4
  )}-${hex.slice(0, 12)}`;
}

/**
 * Extract product data from __NEXT_DATA__ in HTML
 */
function extractProductDataFromNextData(html) {
  try {
    const nextDataMatch = html.match(
      /<script[^>]*id=["']__NEXT_DATA__["'][^>]*>([\s\S]*?)<\/script>/i
    );
    if (nextDataMatch && nextDataMatch[1]) {
      const nextData = JSON.parse(nextDataMatch[1]);
      const productData = nextData?.props?.pageProps?.productData;
      if (productData) {
        console.log(`  ✅ Extracted productData from __NEXT_DATA__`);
        console.log(`  📋 productData keys:`, Object.keys(productData));
        return productData;
      }
    }
  } catch (e) {
    console.log(`  ⚠️ Error extracting __NEXT_DATA__: ${e.message}`);
  }
  return null;
}

/**
 * Extract comprehensive product details from productData object (from __NEXT_DATA__)
 */
function extractFromProductData(productData) {
  if (!productData) return null;

  const parentId = productData.id || "";
  const msid =
    productData.metadata?.pimStyle || productData.metadata?.masterStyle || "";
  const name = productData.name || "";

  // Description from details.longDesc
  const description = productData.details?.longDesc || "";

  // Brand/Designer
  const brand = productData.designer?.name || "Neiman Marcus";

  // Gender
  const gender =
    productData.genderCode === "Women"
      ? "Female"
      : productData.genderCode === "Men"
      ? "Male"
      : "";

  // Category from hierarchy
  let category = "";
  if (productData.hierarchy && productData.hierarchy.length > 0) {
    category = productData.hierarchy[0].level1 || "";
  }

  // Materials from attributesMap
  let materials = "";
  if (productData.attributesMap) {
    const materialFields = [
      productData.attributesMap.firstMaterial,
      productData.attributesMap.Material,
      productData.attributesMap.fabricType,
    ].filter(Boolean);
    if (materialFields.length > 0) {
      materials = Array.isArray(materialFields[0])
        ? materialFields[0].join(", ")
        : materialFields.join(", ");
    }
  }

  // Fit types
  const fitTypes =
    productData.attributesMap?.fitDetails || productData.fitDetails || "";

  // Price
  const originalPrice = parseFloat(productData.price?.retailPrice || 0);
  const salePrice = parseFloat(
    productData.price?.salePrice || productData.price?.promoPrice || 0
  );
  const finalPrice =
    salePrice > 0 && salePrice < originalPrice ? salePrice : originalPrice;
  const isOnSale = salePrice > 0 && salePrice < originalPrice;

  // Extract real images from API for each color
  const colorImages = {};
  if (productData.options?.productOptions) {
    const colorOption = productData.options.productOptions.find(
      (opt) => opt.label === "color"
    );
    if (colorOption && colorOption.values) {
      colorOption.values.forEach((colorVal) => {
        const colorName = colorVal.name || "";
        const images = [];

        // Main image for this color
        if (colorVal.media?.main?.dynamic?.url) {
          images.push(colorVal.media.main.dynamic.url);
        }

        // Real alternate images for this color from API
        if (colorVal.media?.alternate) {
          Object.values(colorVal.media.alternate).forEach((alt) => {
            if (alt?.dynamic?.url) {
              images.push(alt.dynamic.url);
            }
          });
        }

        if (images.length > 0) {
          colorImages[colorName] = images;
        }
      });
    }
  }

  // Fallback: use product-level images
  const productImages = [];
  if (productData.media?.main?.dynamic?.url) {
    productImages.push(productData.media.main.dynamic.url);
  }
  if (productData.media?.alternate) {
    Object.values(productData.media.alternate).forEach((alt) => {
      if (alt?.dynamic?.url) {
        productImages.push(alt.dynamic.url);
      }
    });
  }

  // Build variants from SKUs array
  const variants = [];
  if (productData.skus && Array.isArray(productData.skus)) {
    productData.skus.forEach((sku) => {
      const color = sku.color?.name || "";
      const size = sku.size?.name || "";
      const skuId = sku.id || "";

      // Get real images from API for this color
      let variantImageUrl = "";
      let variantAlternateImages = [];

      // Try to get color-specific images from API
      if (colorImages[color] && colorImages[color].length > 0) {
        variantImageUrl = colorImages[color][0];
        variantAlternateImages = colorImages[color].slice(0);
      } else if (productImages.length > 0) {
        variantImageUrl = productImages[0];
        variantAlternateImages = productImages.slice(0);
      }

      // Ensure image URLs are absolute
      const normalizeImageUrl = (url) => {
        if (!url) return "";
        if (!url.startsWith("http")) {
          return url.startsWith("//")
            ? `https:${url}`
            : `https://www.neimanmarcus.com${url}`;
        }
        return url;
      };

      variantImageUrl = normalizeImageUrl(variantImageUrl);
      variantAlternateImages = variantAlternateImages
        .map(normalizeImageUrl)
        .filter(Boolean);

      // Ensure main image_url is included in alternate_image_urls (as first item)
      if (
        variantImageUrl &&
        !variantAlternateImages.includes(variantImageUrl)
      ) {
        variantAlternateImages.unshift(variantImageUrl);
      } else if (
        variantImageUrl &&
        variantAlternateImages[0] !== variantImageUrl
      ) {
        variantAlternateImages = variantAlternateImages.filter(
          (url) => url !== variantImageUrl
        );
        variantAlternateImages.unshift(variantImageUrl);
      }

      // Availability
      const isInStock = sku.inStock || sku.sellable || false;
      const stockLevel = sku.stockLevel || 0;
      const stockStatusMessage = sku.stockStatusMessage || "";

      // Price (could be per-SKU)
      const variantPrice = sku.price?.retailPrice
        ? parseFloat(sku.price.retailPrice)
        : originalPrice;
      const variantSalePrice =
        sku.price?.salePrice || sku.price?.promoPrice
          ? parseFloat(sku.price.salePrice || sku.price.promoPrice)
          : salePrice;
      const variantFinalPrice =
        variantSalePrice > 0 && variantSalePrice < variantPrice
          ? variantSalePrice
          : variantPrice;
      const variantIsOnSale =
        variantSalePrice > 0 && variantSalePrice < variantPrice;

      const variantUrl = productData.details?.canonicalUrl
        ? `https://www.neimanmarcus.com${productData.details.canonicalUrl}`
        : "";

      variants.push({
        price_currency: "USD",
        original_price: variantPrice,
        link_url: variantUrl,
        deeplink_url: variantUrl,
        image_url: variantImageUrl,
        alternate_image_urls: variantAlternateImages,
        is_on_sale: variantIsOnSale,
        is_in_stock: isInStock,
        size: size,
        color: color,
        mpn: generateUUIDv5(
          `${parentId}-${color}`,
          "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
        ),
        ratings_count: 0,
        average_ratings: 0,
        review_count: 0,
        selling_price: variantPrice,
        sale_price: variantSalePrice,
        final_price: variantFinalPrice,
        discount: variantIsOnSale
          ? Math.round(((variantPrice - variantSalePrice) / variantPrice) * 100)
          : 0,
        operation_type: "INSERT",
        variant_id: generateUUIDv5(
          `${parentId}-${color}-${size}`,
          "6ba7b810-9dad-11d1-80b4-00c04fd430c1"
        ),
        variant_description: "",
      });
    });
  }

  // Format product like Madewell catalog
  return {
    parent_product_id: parentId,
    name: name,
    description: description
      .replace(/<[^>]*>/g, " ")
      .replace(/\s+/g, " ")
      .trim(),
    category: category || "Clothing",
    retailer_domain: "neimanmarcus.com",
    brand: brand,
    gender: gender,
    materials: materials || null,
    fit_types: fitTypes || "",
    return_policy_link:
      "https://www.neimanmarcus.com/assistance/assistance.jsp#returns",
    return_policy:
      "Neiman Marcus offers returns within 30 days of purchase with original receipt.",
    size_chart: null,
    available_bank_offers: "",
    available_coupons: "",
    variants: variants,
    operation_type: "INSERT",
    source: "neimanmarcus",
    msid: msid,
    canonical_url: productData.details?.canonicalUrl || "",
  };
}

/**
 * Fetch product details from __NEXT_DATA__ (productData)
 */
async function fetchProductDetails(product, country = "US") {
  const msid = product.msid || product.id;
  const productId = product.id;
  const canonical = product.canonical || product.url || product.productUrl;
  const url = canonical?.startsWith("http")
    ? canonical
    : `https://www.neimanmarcus.com${canonical}`;

  console.log(`  🔍 Fetching: ${msid} (${productId}) - ${url}`);

  try {
    const response = await retryRequestStopOn403(
      async (axiosInstance) => {
        const axiosResponse = await axiosInstance.get(url, {
          headers: HEADERS,
        });

        // Check for blocking
        if (isBlockedResponse(axiosResponse, axiosResponse.data)) {
          isBlocked = true;
          const error = new Error("403 Forbidden or CAPTCHA detected");
          error.response = axiosResponse;
          throw error;
        }

        return axiosResponse;
      },
      3, // Reduced retries since we stop on 403
      2000,
      country
    );

    if (response && response.data) {
      const html = response.data;
      console.log(`  ✅ HTML fetched (${(html.length / 1024).toFixed(2)} KB)`);

      // Extract productData from __NEXT_DATA__
      const productData = extractProductDataFromNextData(html);

      if (productData) {
        console.log(`  ✅ Extracted productData from __NEXT_DATA__`);
        const formattedProduct = extractFromProductData(productData);

        if (
          formattedProduct &&
          formattedProduct.variants &&
          formattedProduct.variants.length > 0
        ) {
          console.log(
            `  ✅ Product extracted with ${formattedProduct.variants.length} variants`
          );
          return formattedProduct;
        }
      }
    }
  } catch (error) {
    // Check if it's a blocking error
    if (
      error.response?.status === 403 ||
      error.message?.includes("403") ||
      error.message?.includes("CAPTCHA") ||
      error.message?.includes("Forbidden")
    ) {
      isBlocked = true;
      console.log(`  ❌ BLOCKED: 403/CAPTCHA detected - ${error.message}`);
      throw new Error("403 Forbidden or CAPTCHA detected - stopping crawler");
    }

    console.log(`  ❌ HTML fetch failed: ${error.message}`);
  }

  console.log(`  ❌ Could not extract complete product data for ${msid}`);
  return null;
}

/**
 * Fetch all products from listing API - fetches all pages
 */
async function fetchAllProducts(country = "US") {
  const all = [];
  let page = 1;
  let hasMore = true;

  while (hasMore && !isBlocked) {
    const listUrl = `https://www.neimanmarcus.com/c/dt/api/productlisting?categoryId=cat58290731&page=${page}&parentCategoryId=&navPath=`;

    try {
      const response = await retryRequestStopOn403(
        async (axiosInstance) => {
          const axiosResponse = await axiosInstance.get(listUrl, {
            headers: HEADERS,
          });

          // Check for blocking
          if (isBlockedResponse(axiosResponse, axiosResponse.data)) {
            isBlocked = true;
            const error = new Error("403 Forbidden or CAPTCHA detected");
            error.response = axiosResponse;
            throw error;
          }

          return axiosResponse;
        },
        3, // Reduced retries since we stop on 403
        2000,
        country
      );

      if (response && response.data) {
        const data = response.data;
        const products = data?.products || [];

        if (products.length > 0) {
          // Log first product structure for debugging (only on first page)
          if (page === 1 && products[0]) {
            console.log(
              `📋 Sample product from listing API:`,
              Object.keys(products[0])
            );
          }

          console.log(
            `✅ Fetched ${products.length} products from page ${page}`
          );
          all.push(...products);
          page++;

          // Add delay between page requests to avoid rate limits
          await new Promise((r) => setTimeout(r, 1000));
        } else {
          hasMore = false;
        }
      } else {
        hasMore = false;
      }
    } catch (error) {
      // Check if it's a blocking error
      if (
        error.response?.status === 403 ||
        error.message?.includes("403") ||
        error.message?.includes("CAPTCHA") ||
        error.message?.includes("Forbidden")
      ) {
        isBlocked = true;
        console.log(`❌ BLOCKED: 403/CAPTCHA detected - ${error.message}`);
        throw new Error("403 Forbidden or CAPTCHA detected - stopping crawler");
      }

      console.log(`❌ Error fetching page ${page}:`, error.message);
      hasMore = false;
    }
  }

  if (isBlocked) {
    throw new Error(
      "Crawler stopped due to 403/CAPTCHA blocking. Please check your proxy or try again later."
    );
  }

  console.log(`\n✅ Total products collected: ${all.length}`);
  return all;
}

/**
 * Save results to JSON file
 */
function saveJSONFile(data, filename) {
  try {
    const outputDir = path.join(__dirname, "output", "US", "neimanmarcus-US");

    // Create directory if it doesn't exist
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true });
    }

    const filePath = path.join(outputDir, filename);
    fs.writeFileSync(filePath, JSON.stringify(data, null, 2), "utf8");
    console.log(`💾 Saved: ${filePath}`);
    return filePath;
  } catch (error) {
    console.error(`❌ Error saving JSON file: ${error.message}`);
    throw error;
  }
}

/**
 * Test connectivity and check for blocking before starting
 */
async function testConnectivity(country = "US") {
  console.log("🔍 Testing connectivity and checking for blocking...");

  try {
    const testUrl = "https://www.neimanmarcus.com/";
    const response = await retryRequestStopOn403(
      async (axiosInstance) => {
        const axiosResponse = await axiosInstance.get(testUrl, {
          headers: HEADERS,
        });

        // Check for blocking
        if (isBlockedResponse(axiosResponse, axiosResponse.data)) {
          isBlocked = true;
          const error = new Error("403 Forbidden or CAPTCHA detected");
          error.response = axiosResponse;
          throw error;
        }

        return axiosResponse;
      },
      1, // Only 1 attempt - if 403, stop immediately
      2000,
      country
    );

    console.log("✅ Connectivity test passed - no blocking detected\n");
    return true;
  } catch (error) {
    if (
      error.response?.status === 403 ||
      error.message?.includes("403") ||
      error.message?.includes("CAPTCHA") ||
      error.message?.includes("Forbidden")
    ) {
      isBlocked = true;
      console.log("❌ BLOCKED: 403/CAPTCHA detected during connectivity test");
      console.log(
        "Cannot proceed with crawling. Please check your proxy or try again later."
      );
      return false;
    }
    console.log(`⚠️ Connectivity test warning: ${error.message}`);
    return true; // Continue anyway for other errors
  }
}

/**
 * Main execution
 */
async function main() {
  console.log("🚀 Starting Neiman Marcus crawler...");
  console.log("📌 Processing all products in chunks of 500\n");

  try {
    // Test connectivity first
    const canProceed = await testConnectivity("US");
    if (!canProceed || isBlocked) {
      console.log(
        "\n❌ Cannot proceed: Blocking detected during connectivity test."
      );
      process.exit(1);
    }

    // Fetch all products
    const allProducts = await fetchAllProducts("US");

    if (isBlocked) {
      console.log(
        "\n❌ Crawler stopped: 403/CAPTCHA detected. Please check your proxy or try again later."
      );
      process.exit(1);
    }

    const CHUNK_SIZE = 500;
    const totalChunks = Math.ceil(allProducts.length / CHUNK_SIZE);

    console.log(`\n📊 Total products to process: ${allProducts.length}`);
    console.log(
      `📦 Will process in ${totalChunks} chunk(s) of ${CHUNK_SIZE} products\n`
    );

    // Process products in chunks
    for (let chunkIndex = 0; chunkIndex < totalChunks; chunkIndex++) {
      if (isBlocked) {
        console.log(
          "\n❌ Crawler stopped: 403/CAPTCHA detected during processing."
        );
        break;
      }

      const startIndex = chunkIndex * CHUNK_SIZE;
      const endIndex = Math.min(startIndex + CHUNK_SIZE, allProducts.length);
      const chunkProducts = allProducts.slice(startIndex, endIndex);

      console.log(
        `\n🔄 Processing chunk ${chunkIndex + 1}/${totalChunks} (products ${
          startIndex + 1
        }-${endIndex})`
      );
      console.log(
        `🔍 Fetching details for ${chunkProducts.length} products...\n`
      );

      const results = [];

      for (let i = 0; i < chunkProducts.length; i++) {
        if (isBlocked) {
          console.log("\n❌ Stopping chunk processing due to blocking.");
          break;
        }

        const p = chunkProducts[i];
        try {
          // First, try to create product from listing data if it has enough info
          let formattedProduct = null;

          // Check if listing product has enough data
          if (p.name && (p.rprc || p.msid)) {
            const parentId = p.id || p.msid || "";
            const msid = p.msid || "";

            // Extract price
            const priceStr = (p.rprc || "").toString().replace(/[^0-9.]/g, "");
            const originalPrice = parseFloat(priceStr) || 0;
            const salePriceStr = (p.sprc || p.salePrice || "")
              .toString()
              .replace(/[^0-9.]/g, "");
            const salePrice = parseFloat(salePriceStr) || 0;
            const finalPrice =
              salePrice > 0 && salePrice < originalPrice
                ? salePrice
                : originalPrice;
            const isOnSale = salePrice > 0 && salePrice < originalPrice;

            // Build URL
            const canonicalPath = p.canonical || "";
            const linkUrl = canonicalPath.startsWith("http")
              ? canonicalPath
              : `https://www.neimanmarcus.com${canonicalPath}`;

            // Extract image
            let imageUrl = "";
            let alternateImages = [];
            if (p.main) {
              let baseImageUrl = "";
              if (typeof p.main === "string") {
                if (
                  p.main.startsWith("http://") ||
                  p.main.startsWith("https://")
                ) {
                  baseImageUrl = p.main;
                } else if (p.main.startsWith("//")) {
                  baseImageUrl = `https:${p.main}`;
                } else if (p.main.startsWith("/")) {
                  baseImageUrl = `https://www.neimanmarcus.com${p.main}`;
                } else {
                  baseImageUrl = `https://www.neimanmarcus.com/${p.main}`;
                }
              } else if (p.main.url) {
                let mainUrl = p.main.url;
                if (mainUrl.startsWith("//")) {
                  mainUrl = `https:${mainUrl}`;
                } else if (!mainUrl.startsWith("http")) {
                  mainUrl = `https://www.neimanmarcus.com${
                    mainUrl.startsWith("/") ? "" : "/"
                  }${mainUrl}`;
                }
                baseImageUrl = mainUrl;
              }

              if (baseImageUrl) {
                const imageVariants = generateImageVariants(
                  baseImageUrl,
                  msid,
                  null
                );
                if (imageVariants.length > 0) {
                  imageUrl = imageVariants[0];
                  alternateImages = imageVariants.slice(1);
                } else {
                  imageUrl = baseImageUrl;
                  alternateImages = [];
                }
              }
            }

            // Extract color
            const color = p.clrName || p.color || "Default";

            // Create a basic variant from listing data
            const variants = [
              {
                price_currency: "USD",
                original_price: originalPrice,
                link_url: linkUrl,
                deeplink_url: linkUrl,
                image_url: imageUrl,
                alternate_image_urls: alternateImages,
                is_on_sale: isOnSale,
                is_in_stock: true,
                size: "One Size",
                color: color,
                mpn: generateUUIDv5(
                  `${parentId}-${color}`,
                  "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
                ),
                ratings_count: 0,
                average_ratings: 0,
                review_count: 0,
                selling_price: originalPrice,
                sale_price: salePrice,
                final_price: finalPrice,
                discount: isOnSale
                  ? Math.round(
                      ((originalPrice - salePrice) / originalPrice) * 100
                    )
                  : 0,
                operation_type: "INSERT",
                variant_id: generateUUIDv5(
                  `${parentId}-${color}-One Size`,
                  "6ba7b810-9dad-11d1-80b4-00c04fd430c1"
                ),
                variant_description: "",
              },
            ];

            formattedProduct = {
              parent_product_id: parentId,
              name: p.name || "",
              description: "",
              category: "Clothing",
              retailer_domain: "neimanmarcus.com",
              brand: p.designer || p.brand || "Neiman Marcus",
              gender: "Women",
              materials: "",
              return_policy_link:
                "https://www.neimanmarcus.com/assistance/assistance.jsp#returns",
              return_policy:
                "Neiman Marcus offers returns within 30 days of purchase with original receipt.",
              size_chart: null,
              available_bank_offers: "",
              available_coupons: "",
              variants: variants,
              operation_type: "INSERT",
              source: "neimanmarcus",
            };

            // Try to enrich with detail API/HTML data
            try {
              const detailProduct = await fetchProductDetails(p, "US");
              if (detailProduct) {
                if (detailProduct.description) {
                  formattedProduct.description = detailProduct.description;
                }
                if (detailProduct.materials) {
                  formattedProduct.materials = detailProduct.materials;
                }
                if (detailProduct.gender) {
                  formattedProduct.gender = detailProduct.gender;
                }
                if (detailProduct.fit_types) {
                  formattedProduct.fit_types = detailProduct.fit_types;
                }
                if (
                  detailProduct.category &&
                  detailProduct.category !== "Clothing"
                ) {
                  formattedProduct.category = detailProduct.category;
                }
                // Always use detail variants if available
                if (
                  detailProduct.variants &&
                  detailProduct.variants.length > 0
                ) {
                  formattedProduct.variants = detailProduct.variants;
                  console.log(
                    `  ✅ Enriched with ${detailProduct.variants.length} variants from detail API`
                  );
                }
              }
            } catch (e) {
              if (
                e.message?.includes("403") ||
                e.message?.includes("CAPTCHA")
              ) {
                throw e; // Re-throw blocking errors
              }
              console.log(
                `  ⚠️ Could not enrich with detail data: ${e.message}`
              );
            }
          }

          // If listing data wasn't enough, try fetching details
          if (
            !formattedProduct ||
            !formattedProduct.variants ||
            formattedProduct.variants.length === 0
          ) {
            formattedProduct = await fetchProductDetails(p, "US");
          }

          if (
            formattedProduct &&
            formattedProduct.variants &&
            formattedProduct.variants.length > 0
          ) {
            results.push(formattedProduct);
            console.log(
              `✔ [${i + 1}/${chunkProducts.length}] ${
                formattedProduct.parent_product_id
              } - ${formattedProduct.name} (${
                formattedProduct.variants.length
              } variants)`
            );
          } else {
            console.log(
              `⚠️ [${i + 1}/${chunkProducts.length}] ${
                p.id || p.msid
              } - No variants found, skipping`
            );
          }

          // Delay between product requests
          await new Promise((r) => setTimeout(r, 500));
        } catch (err) {
          if (
            err.message?.includes("403") ||
            err.message?.includes("CAPTCHA") ||
            err.message?.includes("Forbidden")
          ) {
            throw err; // Re-throw blocking errors
          }
          console.log(
            `❌ [${i + 1}/${chunkProducts.length}] ${p.id || p.msid} failed:`,
            err.message
          );
        }
      }

      // Save chunk results
      if (results.length > 0 && !isBlocked) {
        const timestamp = new Date()
          .toISOString()
          .replace(/[:.]/g, "-")
          .slice(0, -5);
        const filename = `catalog-chunk-${chunkIndex + 1}-of-${totalChunks}-${
          startIndex + 1
        }-to-${endIndex}-${timestamp}.json`;

        saveJSONFile(results, filename);
        console.log(
          `\n💾 Chunk ${chunkIndex + 1} JSON file saved: ${filename}`
        );
        console.log(
          `📊 Chunk ${chunkIndex + 1} contains ${results.length} products`
        );
        console.log(
          `📦 Chunk ${chunkIndex + 1} total variants: ${results.reduce(
            (sum, p) => sum + (p.variants?.length || 0),
            0
          )}`
        );
      } else if (isBlocked) {
        console.log(
          `\n⚠️ Chunk ${chunkIndex + 1} processing stopped due to blocking`
        );
      } else {
        console.log(`\n⚠️ Chunk ${chunkIndex + 1} has no results to save`);
      }

      // Add delay between chunks to avoid rate limits (except for the last chunk)
      if (chunkIndex < totalChunks - 1 && !isBlocked) {
        console.log(`\n⏳ Waiting 5 seconds before processing next chunk...`);
        await new Promise((r) => setTimeout(r, 5000));
      }
    }

    if (!isBlocked) {
      console.log("\n✅ All chunks processed!");
      console.log(`📊 Total products processed: ${allProducts.length}`);
    } else {
      console.log("\n❌ Crawler stopped due to 403/CAPTCHA blocking.");
      process.exit(1);
    }
  } catch (error) {
    if (
      error.message?.includes("403") ||
      error.message?.includes("CAPTCHA") ||
      error.message?.includes("Forbidden")
    ) {
      console.log(
        "\n❌ FATAL ERROR: 403/CAPTCHA detected. Crawler cannot continue."
      );
      console.log("Please check your proxy configuration or try again later.");
      process.exit(1);
    }
    console.error("\n❌ Fatal error:", error);
    process.exit(1);
  }
}

// Run the crawler
if (require.main === module) {
  main().catch((error) => {
    console.error("Unhandled error:", error);
    process.exit(1);
  });
}

module.exports = {
  fetchAllProducts,
  fetchProductDetails,
  extractFromProductData,
  extractProductDataFromNextData,
  generateImageVariants,
  generateUUIDv5,
  isBlockedResponse,
  testConnectivity,
  main,
};
