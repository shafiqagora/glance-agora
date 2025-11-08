/** @format */
/**
 * Madewell FAST API Crawler
 *
 * HYBRID APPROACH:
 * 1. Launch browser ONCE to get JWT token
 * 2. Use Puppeteer for category listing
 * 3. Use API for ALL product details (NO clicking!)
 *
 * 10-50x FASTER than old clicking approach!
 */

require("dotenv").config();
const axios = require("axios");
const { v5: uuidv5 } = require("uuid");
const fs = require("fs");
const path = require("path");
const puppeteer = require("puppeteer");
const { retryPuppeteerWithProxyRotation } = require("./utils/helper");

axios.defaults.timeout = 180000;

// ============================================================================
// CONFIGURATION
// ============================================================================

const STORE_CONFIG = {
  MADEWELL: {
    name: "Madewell",
    domain: "madewell.com",
    currency: "USD",
    country: "US",
    returnPolicy: "https://www.madewell.com/us/c/returns/",
  },
};

const MADEWELL_CATEGORIES = {
  WOMENS_NEW_ARRIVALS: {
    name: "Women's New Arrivals",
    url: "/us/womens/new/new-arrivals/",
  },
  MENS_NEW_ARRIVALS: {
    name: "Men's New Arrivals",
    url: "/us/mens/new/new-arrivals/",
  },
  WOMENS_SALE: {
    name: "Women's Sale",
    url: "/us/womens/sale/",
  },
  MENS_SALE: {
    name: "Men's Sale",
    url: "/us/mens/sale/",
  },
  WOMENS_JEANS: {
    name: "Women's Jeans",
    url: "/us/womens/clothing/jeans/",
  },
  MENS_JEANS: {
    name: "Men's Jeans",
    url: "/us/mens/clothing/jeans/",
  },
};

const IMAGE_BASE_URL = "https://www.madewell.com/images";

// ============================================================================
// FILE SERVICE FUNCTIONS
// ============================================================================

function createOutputDirectory(countryCode, brandName) {
  const dirPath = path.join(
    __dirname,
    "output",
    countryCode,
    `${brandName}-${countryCode}`
  );

  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }

  return dirPath;
}

function startIncrementalCatalog(countryCode, brandName, storeData = {}) {
  const dirPath = createOutputDirectory(countryCode || "US", brandName);
  const jsonPath = path.join(dirPath, "catalog.json");

  const header = {
    store_info: {
      name: storeData.name || brandName,
      domain: storeData.domain || `${brandName}.com`,
      currency: storeData.currency || "USD",
      country: countryCode || "US",
      crawled_at: new Date().toISOString(),
    },
    products: [],
  };

  const headerString = JSON.stringify(
    { store_info: header.store_info },
    null,
    2
  );
  const prefix = headerString.replace(/}\s*$/, "") + ',\n  "products": [\n';
  fs.writeFileSync(jsonPath, prefix, "utf8");

  return { dirPath, jsonPath, wroteFirst: false };
}

function appendProductIncremental(ctx, product) {
  if (!ctx || !ctx.jsonPath) throw new Error("Invalid context");

  const formattedProduct = JSON.stringify(product, null, 4);
  const jsonLine = (ctx.wroteFirst ? ",\n" : "") + formattedProduct;
  fs.appendFileSync(ctx.jsonPath, jsonLine, "utf8");

  ctx.wroteFirst = true;
}

function finalizeIncrementalCatalog(ctx) {
  if (!ctx || !ctx.jsonPath) throw new Error("Invalid context");

  fs.appendFileSync(ctx.jsonPath, "\n  ]\n}", "utf8");

  return {
    jsonPath: ctx.jsonPath,
    dirPath: ctx.dirPath,
  };
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

async function closeAllModals(page) {
  try {
    const shipToModal = await page.$(
      "button.ShipToModal_continueShoppingBtn__QubmY"
    );
    if (shipToModal) {
      await shipToModal.click();
      await page
        .waitForSelector("button.ShipToModal_continueShoppingBtn__QubmY", {
          hidden: true,
          timeout: 3000,
        })
        .catch(() => {});
    }
  } catch {}

  try {
    const signUpModal = await page.$('[data-testid="domestic-auth-modal"]');
    if (signUpModal) {
      const closeBtn = await signUpModal.$('button[aria-label="Close modal"]');
      if (closeBtn) {
        await closeBtn.click();
        await page
          .waitForSelector('[data-testid="domestic-auth-modal"]', {
            hidden: true,
            timeout: 3000,
          })
          .catch(() => {});
      }
    }
  } catch {}

  try {
    const modalClose = await page.$('[data-testid="modal-close"]');
    if (modalClose) {
      await modalClose.click();
      await page
        .waitForSelector('[data-testid="modal-close"]', {
          hidden: true,
          timeout: 3000,
        })
        .catch(() => {});
    }
  } catch {}
}

// ============================================================================
// JWT TOKEN MANAGEMENT
// ============================================================================

/**
 * Extract JWT token by intercepting API requests
 * ⭐ Capture the token from the Authorization header when browser calls the API
 */
async function extractJWTTokenFromRequest(page) {
  console.log("🔍 Intercepting API requests to capture JWT token...");

  return new Promise((resolve, reject) => {
    let jwtToken = null;
    let allCookies = null;

    // Enable request interception
    page.on("request", (request) => {
      request.continue();
    });

    // Capture responses to find the JWT token
    page.on("response", async (response) => {
      try {
        const url = response.url();

        // Look for the /browse/products API call
        if (url.includes("/browse/products")) {
          console.log(`   📡 Captured API call: ${url}`);

          // Get the Authorization header from the request
          const request = response.request();
          const headers = request.headers();

          if (headers.authorization) {
            jwtToken = headers.authorization.replace("Bearer ", "");
            console.log(`   ✅ JWT token captured from Authorization header!`);
            console.log(`      Token length: ${jwtToken.length} characters`);
            console.log(`      Token preview: ${jwtToken.substring(0, 50)}...`);

            // Get cookies
            const cookies = await page.cookies();
            allCookies = cookies.map((c) => `${c.name}=${c.value}`).join("; ");

            resolve({ jwtToken, allCookies });
          }
        }
      } catch (error) {
        // Ignore errors in response handler
      }
    });

    // Timeout after 30 seconds
    setTimeout(() => {
      if (!jwtToken) {
        reject(new Error("Failed to capture JWT token from API requests"));
      }
    }, 30000);
  });
}

// ============================================================================
// API FUNCTIONS
// ============================================================================

/**
 * Construct image URLs from product ID, color code, and shot types
 * Madewell format: https://www.madewell.com/images/NY009_DM8769_m
 */
function constructImageUrls(productId, colorCode, shotTypes) {
  return shotTypes.map((shotType) => {
    // Remove any file extension (.jpg) if present in shotType
    const cleanShotType = shotType.replace(/\.(jpg|jpeg|png)$/i, "");
    // Return clean URL without extension (Madewell serves images without extension in URL)
    return `${IMAGE_BASE_URL}/${productId}_${colorCode}${cleanShotType}`;
  });
}

/**
 * Extract materials from shortDescription HTML
 * Example: "66% cotton/20% lyocell/14% wool" from <li> tags
 */
function extractMaterialsFromDescription(shortDescription) {
  if (!shortDescription) return "";

  // Remove HTML tags
  const text = shortDescription
    .replace(/<[^>]*>/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  // Look for material percentages (e.g., "66% cotton/20% lyocell")
  const materialMatch = text.match(/(\d+%\s*[^./]+(?:\/\d+%\s*[^./]+)*)/i);
  if (materialMatch) {
    return materialMatch[1].trim().replace(/\s+/g, " ");
  }

  return "";
}

/**
 * Normalize color name for consistent MPN generation
 * Ensures variants with the same color name have the same MPN
 */
function normalizeColorName(colorName) {
  if (!colorName) return "NO_COLOR";
  return colorName
    .toUpperCase()
    .trim()
    .replace(/\s+/g, " ") // Normalize whitespace
    .replace(/[^\w\s-]/g, ""); // Remove special characters except hyphens
}

/**
 * Parse fit types from c_customData.extendedSizing
 * Returns a string like "Standard, Plus, Petite, Tall (NY009, NY578, NY577, NY576)"
 */
function parseFitTypes(extendedSizing) {
  if (!extendedSizing || !Array.isArray(extendedSizing)) return "";

  const fitTypes = extendedSizing
    .map((fit) => `${fit.name} (${fit.pid})`)
    .join(", ");

  return fitTypes;
}

/**
 * Fetch product reviews/ratings from API
 */
async function fetchProductReviews(productId) {
  try {
    // Madewell uses Bazaarvoice for reviews
    const url = `https://api.bazaarvoice.com/data/reviews.json?apiversion=5.4&passkey=caOfotGbpJrPdzMJM9k45irxAL9aJwqQjkllFBwqdhV9I&Filter=ProductId:${productId}&Stats=Reviews&Include=Products`;

    const response = await axios.get(url, {
      timeout: 10000,
    });

    const reviewStats =
      response.data?.Includes?.Products?.[productId]?.ReviewStatistics;

    if (reviewStats) {
      return {
        average_ratings: reviewStats.AverageOverallRating || 0,
        ratings_count: reviewStats.TotalReviewCount || 0,
        review_count: reviewStats.TotalReviewCount || 0,
      };
    }

    return {
      average_ratings: 0,
      ratings_count: 0,
      review_count: 0,
    };
  } catch (error) {
    // Reviews are optional, don't log errors
    return {
      average_ratings: 0,
      ratings_count: 0,
      review_count: 0,
    };
  }
}

/**
 * Fetch product details from API
 */
async function fetchProductDetailsFromAPI(productIds, jwtToken, allCookies) {
  try {
    const ids = Array.isArray(productIds) ? productIds.join(",") : productIds;
    const url = `https://www.madewell.com/browse/products?expand=availability,variations,prices,options,images&c_country-code=US&ids=${ids}`;

    const response = await axios.get(url, {
      headers: {
        Authorization: `Bearer ${jwtToken}`,
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        Accept: "application/json",
        "Content-Type": "application/json",
        Cookie: allCookies,
        Referer: "https://www.madewell.com/",
        "Accept-Language": "en-US,en;q=0.9",
      },
      timeout: 30000,
    });

    return response.data.data || [];
  } catch (error) {
    console.error(`   ❌ API Error: ${error.message}`);
    if (error.response?.status) {
      console.error(`      Status: ${error.response.status}`);
    }
    return [];
  }
}

/**
 * Parse API product data into our format
 */
function parseAPIProduct(apiProduct, reviewData = null) {
  try {
    const masterId = apiProduct.id;
    const productName = apiProduct.name || "";
    const basePrice = apiProduct.price || 0;
    const currency = apiProduct.currency || "USD";
    const longDescription = apiProduct.longDescription || "";
    const shortDescription = (apiProduct.shortDescription || "")
      .replace(/<[^>]*>/g, " ")
      .replace(/\s+/g, " ")
      .trim();

    const colorsData = apiProduct.c_customData?.colors || [];
    const defaultShotTypes = apiProduct.c_shotType || ["_m"];

    // Check for fit type variations (Standard, Plus, Petite, Tall)
    const fitTypeAttribute = apiProduct.variationAttributes?.find(
      (attr) =>
        attr.id === "fitType" ||
        attr.id === "fit_type" ||
        attr.id === "size_fit"
    );
    const fitTypes = fitTypeAttribute?.values || [];

    // Get review/rating data
    const ratings = reviewData || {
      average_ratings: 0,
      ratings_count: 0,
      review_count: 0,
    };

    const variants = [];

    // Extract variants from customData colors
    for (const colorData of colorsData) {
      const colorCode = colorData.value;
      const colorName = colorData.name;
      const colorPrice = colorData.price || basePrice;
      const shotTypes = colorData.shotType || defaultShotTypes;

      const imageUrls = constructImageUrls(masterId, colorCode, shotTypes);

      const colorVariants = (apiProduct.variants || []).filter(
        (v) => v.variationValues?.color === colorCode
      );

      for (const variant of colorVariants) {
        const size = variant.variationValues?.size || "";
        const fitType =
          variant.variationValues?.fitType ||
          variant.variationValues?.fit_type ||
          variant.variationValues?.size_fit ||
          "";

        // Get variant-specific images if available
        let variantImageUrls = imageUrls;
        if (apiProduct.imageGroups) {
          const variantImageGroup = apiProduct.imageGroups.find((group) =>
            group.variationAttributes?.some(
              (attr) => attr.id === "color" && attr.values?.includes(colorCode)
            )
          );

          if (variantImageGroup?.images) {
            variantImageUrls = variantImageGroup.images
              .filter((img) => img.link)
              .map((img) => img.link);

            // Fallback to constructed URLs if no images found
            if (variantImageUrls.length === 0) {
              variantImageUrls = imageUrls;
            }
          }
        }

        // Build variant description with fit type if available
        let variantDescription = "";
        if (fitType) {
          variantDescription = `Fit: ${fitType}`;
        }

        // Normalize color name for consistent MPN generation
        const normalizedColorName = normalizeColorName(colorName);

        variants.push({
          price_currency: "USD",
          original_price: colorPrice,
          link_url: `https://www.madewell.com/US/p/${masterId}/?color=${colorCode}&countryCode=US&pidUri=${masterId}&ccode=${colorCode}`,
          deeplink_url: `https://www.madewell.com/US/p/${masterId}/?color=${colorCode}&countryCode=US&pidUri=${masterId}&ccode=${colorCode}`,
          image_url: variantImageUrls[0] || "",
          alternate_image_urls: variantImageUrls,
          is_on_sale: false,
          is_in_stock: variant.orderable || false,
          size: size,
          color: colorName,
          mpn: uuidv5(
            `${masterId}-${normalizedColorName}`,
            "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
          ),
          ratings_count: ratings.ratings_count,
          average_ratings: ratings.average_ratings,
          review_count: ratings.review_count,
          selling_price: colorPrice,
          sale_price: 0,
          final_price: colorPrice,
          discount: 0,
          operation_type: "INSERT",
          variant_id: uuidv5(
            `${masterId}-${colorCode}-${size}${fitType ? `-${fitType}` : ""}`,
            "6ba7b810-9dad-11d1-80b4-00c04fd430c1"
          ),
          variant_description: variantDescription,
        });
      }
    }

    // Fallback to variationAttributes if no customData
    if (variants.length === 0 && apiProduct.variationAttributes) {
      const colors =
        apiProduct.variationAttributes.find((attr) => attr.id === "color")
          ?.values || [];
      const sizes =
        apiProduct.variationAttributes.find((attr) => attr.id === "size")
          ?.values || [];

      for (const color of colors) {
        const colorCode = color.value;
        const colorName = color.name;
        const imageUrls = constructImageUrls(
          masterId,
          colorCode,
          defaultShotTypes
        );

        for (const size of sizes) {
          const variant = (apiProduct.variants || []).find(
            (v) =>
              v.variationValues?.color === colorCode &&
              v.variationValues?.size === size.value
          );

          if (variant) {
            const fitType =
              variant.variationValues?.fitType ||
              variant.variationValues?.fit_type ||
              variant.variationValues?.size_fit ||
              "";

            let variantDescription = "";
            if (fitType) {
              variantDescription = `Fit: ${fitType}`;
            }

            // Normalize color name for consistent MPN generation
            const normalizedColorName = normalizeColorName(colorName);

            variants.push({
              price_currency: "USD",
              original_price: variant.price || basePrice,
              link_url: `https://www.madewell.com/US/p/${masterId}/?color=${colorCode}&countryCode=US&pidUri=${masterId}&ccode=${colorCode}`,
              deeplink_url: `https://www.madewell.com/US/p/${masterId}/?color=${colorCode}&countryCode=US&pidUri=${masterId}&ccode=${colorCode}`,
              image_url: imageUrls[0] || "",
              alternate_image_urls: imageUrls,
              is_on_sale: false,
              is_in_stock: variant.orderable || false,
              size: size.value,
              color: colorName,
              mpn: uuidv5(
                `${masterId}-${normalizedColorName}`,
                "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
              ),
              ratings_count: ratings.ratings_count,
              average_ratings: ratings.average_ratings,
              review_count: ratings.review_count,
              selling_price: variant.price || basePrice,
              sale_price: 0,
              final_price: variant.price || basePrice,
              discount: 0,
              operation_type: "INSERT",
              variant_id: uuidv5(
                `${masterId}-${colorCode}-${size.value}${
                  fitType ? `-${fitType}` : ""
                }`,
                "6ba7b810-9dad-11d1-80b4-00c04fd430c1"
              ),
              variant_description: variantDescription,
            });
          }
        }
      }
    }

    // Extract materials using improved function
    const materials = extractMaterialsFromDescription(
      apiProduct.shortDescription
    );

    // Combine longDescription and shortDescription
    const fullDescription = longDescription
      ? `${longDescription} ${shortDescription}`.trim()
      : shortDescription;

    // Get fit types info from extendedSizing
    const fitTypesInfo = parseFitTypes(apiProduct.c_customData?.extendedSizing);

    // Determine gender
    let gender = "";
    const categories = apiProduct.c_categories || [];
    if (categories.some((c) => c.includes("womens"))) gender = "Female";
    else if (categories.some((c) => c.includes("mens"))) gender = "Male";

    return {
      parent_product_id: masterId,
      name: productName,
      description: fullDescription,
      category: categories[0] || "",
      retailer_domain: "madewell.com",
      brand: "Madewell",
      gender: gender,
      materials: materials,
      fit_types: fitTypesInfo, // Add fit types information
      return_policy_link: "https://www.madewell.com/us/c/returns/",
      return_policy:
        "Madewell gladly accepts returns or exchanges of merchandise purchased online within 30 days of original purchase. Final sale items cannot be returned or exchanged. Free returns on all orders.",
      size_chart: null,
      available_bank_offers: "",
      available_coupons: "",
      variants: variants,
      operation_type: "INSERT",
      source: "madewell",
    };
  } catch (error) {
    console.error(
      `   ⚠️ Error parsing product ${apiProduct.id}: ${error.message}`
    );
    return null;
  }
}

// ============================================================================
// CATEGORY LISTING (Puppeteer)
// ============================================================================

/**
 * Fetch product list from category using Puppeteer
 */
async function fetchMadewellProductList(categoryUrl, minProducts = 5) {
  console.log(`📋 Fetching product list from: ${categoryUrl}`);

  const allProducts = [];
  let currentPage = 1;

  try {
    const result = await retryPuppeteerWithProxyRotation(
      async (browser) => {
        const page = await browser.newPage();

        await page.setViewport({ width: 1920, height: 1080 });
        await page.setUserAgent(
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        );

        // Set cookies
        await page.setCookie(
          {
            name: "country",
            value: "US",
            domain: ".madewell.com",
            path: "/",
          },
          {
            name: "currency",
            value: "USD",
            domain: ".madewell.com",
            path: "/",
          },
          {
            name: "locale",
            value: "en-US",
            domain: ".madewell.com",
            path: "/",
          },
          {
            name: "shipToCountry",
            value: "US",
            domain: ".madewell.com",
            path: "/",
          },
          {
            name: "hasShownShipToModal",
            value: "true",
            domain: ".madewell.com",
            path: "/",
          }
        );

        const fullUrl = `https://www.madewell.com${categoryUrl}?country=US&currency=USD`;
        console.log(`🌐 Navigating to: ${fullUrl}`);
        await page.goto(fullUrl, {
          waitUntil: "domcontentloaded",
          timeout: 120000,
        });

        // Close modals
        await closeAllModals(page);

        // Wait for product grid
        await page.waitForSelector("ul.ProductsGrid_plpGrid__OP3wT", {
          visible: true,
        });

        // Get total products count
        let totalProducts = 0;
        try {
          await page.waitForSelector(
            ".ResultsCount_filtersResultsCount__KZ39j",
            {
              visible: true,
            }
          );
          const totalText = await page.$eval(
            ".ResultsCount_filtersResultsCount__KZ39j",
            (el) => el.textContent
          );
          const match = totalText.match(/(\d+)/);
          if (match) {
            totalProducts = parseInt(match[1]);
            console.log(`📦 Total products available: ${totalProducts}`);
          }
        } catch {}

        // Fetch products from all pages
        while (
          allProducts.length < Math.min(minProducts, totalProducts || 2000)
        ) {
          console.log(`📄 Page ${currentPage}...`);

          await page.waitForSelector("li.ProductsGrid_plpGridElement__aSWFa", {
            visible: true,
          });

          // Extract product URLs from current page
          const pageProducts = await page.evaluate(() => {
            const products = [];
            const productElements = document.querySelectorAll(
              "li.ProductsGrid_plpGridElement__aSWFa"
            );

            productElements.forEach((el) => {
              try {
                const productDiv = el.querySelector("[data-cnstrc-item-id]");
                const productId =
                  productDiv?.getAttribute("data-cnstrc-item-id") || "";
                const productName =
                  productDiv?.getAttribute("data-cnstrc-item-name") || "";

                const link = el.querySelector(
                  'a.ProductTile_productTileImgLink__6VjK2[href*="/p/"]'
                );
                let productUrl = link?.getAttribute("href") || "";
                if (productUrl && productUrl.startsWith("/")) {
                  productUrl = new URL(productUrl, window.location.origin).href;
                }

                if (productId && productUrl && productName) {
                  products.push({
                    id: productId,
                    name: productName,
                    productUrl: productUrl,
                  });
                }
              } catch (e) {}
            });

            return products;
          });

          console.log(`  ✅ Found ${pageProducts.length} products`);
          allProducts.push(...pageProducts);

          if (allProducts.length >= minProducts) {
            console.log(`🎯 Reached target of ${minProducts} products`);
            break;
          }

          // Close all modals before pagination
          await closeAllModals(page);

          // ⭐ Navigate to next page - CHECK IF DISABLED FIRST (NO TIMEOUT!)
          try {
            const nextButton = await page.$(
              "a.Pagination_plpPaginationNext__2S2dh"
            );

            if (!nextButton) {
              console.log("📭 No next button found - end of catalog");
              break;
            }

            // ⭐ Check if next button is disabled
            const isDisabled = await nextButton.evaluate((el) => {
              return (
                el.getAttribute("aria-disabled") === "true" ||
                el.classList.contains("disabled") ||
                el.hasAttribute("disabled")
              );
            });

            if (isDisabled) {
              console.log(
                "📭 Next button is disabled - reached end of catalog"
              );
              break;
            }

            const nextHref = await nextButton.evaluate((el) =>
              el.getAttribute("href")
            );

            if (!nextHref) {
              console.log("📭 No href on next button - end of catalog");
              break;
            }

            const url = new URL(nextHref, "https://www.madewell.com");
            url.searchParams.set("country", "US");
            url.searchParams.set("currency", "USD");

            console.log(`➡️  Next page: ${currentPage + 1}`);

            // ⭐ Navigate WITHOUT timeout - just wait for products to appear
            await page.goto(url.toString(), {
              waitUntil: "domcontentloaded",
              // NO timeout parameter - let it take as long as needed
            });

            // Close all modals after pagination
            await closeAllModals(page);

            // ⭐ Wait for products to load (this is the real check)
            await page.waitForSelector(
              "li.ProductsGrid_plpGridElement__aSWFa",
              {
                visible: true,
                timeout: 30000, // 30 seconds is enough for products to load
              }
            );

            currentPage++;
          } catch (error) {
            console.log(`❌ Pagination error: ${error.message}`);
            console.log(
              "📭 Stopping pagination - may have reached end or network issue"
            );
            break;
          }
        }

        await page.close();
        return allProducts;
      },
      3,
      2000,
      "US"
    );

    return result;
  } catch (error) {
    console.error(`Error fetching product list: ${error.message}`);
    return [];
  }
}

// ============================================================================
// MAIN CRAWLER FUNCTION
// ============================================================================

async function madewellMain(minProductsPerCategory = 1600) {
  const store = STORE_CONFIG.MADEWELL;
  const inc = startIncrementalCatalog(store.country, "madewell", store);

  let totalProducts = 0;
  let categoryCount = 0;
  const totalCategories = Object.keys(MADEWELL_CATEGORIES).length;

  const processedProductIds = new Set();

  // ⭐ Set to null to use dynamic token extraction (RECOMMENDED)
  // ⭐ Or set to a valid token for quick testing
  const HARDCODED_TOKEN = null; // Changed from hardcoded token to null to use dynamic extraction

  let jwtToken = null;
  let allCookies = null;

  // Try to use hardcoded token first
  if (HARDCODED_TOKEN) {
    console.log("\n🔑 USING HARDCODED TOKEN (for testing)...\n");
    jwtToken = HARDCODED_TOKEN;
    allCookies = `mw.t=${HARDCODED_TOKEN}; country=US; currency=USD; locale=en-US`;
    console.log("✅ Token ready for API calls!\n");
  } else {
    // Fallback: Launch browser ONCE to get JWT token
    console.log("\n🚀 LAUNCHING BROWSER TO GET JWT TOKEN...\n");

    try {
      const tokenResult = await retryPuppeteerWithProxyRotation(
        async (browser) => {
          const page = await browser.newPage();

          await page.setViewport({ width: 1920, height: 1080 });
          await page.setUserAgent(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
          );

          // ⭐ Enable request interception BEFORE navigating
          await page.setRequestInterception(true);

          // Set cookies
          await page.setCookie(
            {
              name: "country",
              value: "US",
              domain: ".madewell.com",
              path: "/",
            },
            {
              name: "currency",
              value: "USD",
              domain: ".madewell.com",
              path: "/",
            }
          );

          // Start intercepting (this returns a promise that resolves when token is captured)
          const tokenPromise = extractJWTTokenFromRequest(page);

          // Navigate to a product details page - this will trigger the API call
          console.log("🌐 Loading a product page to trigger API call...");
          await page.goto(
            "https://www.madewell.com/p/womens/clothing/jeans/wide-leg-jeans/the-perfect-vintage-wide-leg-jean-in-softdrape/NY009/?ccode=DM8769",
            {
              waitUntil: "domcontentloaded",
              timeout: 60000,
            }
          );

          // Wait for token to be captured from the intercepted API call
          const tokenData = await tokenPromise;

          await page.close();

          return tokenData;
        },
        3,
        2000,
        "US"
      );

      jwtToken = tokenResult.jwtToken;
      allCookies = tokenResult.allCookies;

      console.log("✅ JWT token ready for API calls!\n");
    } catch (error) {
      console.error(`❌ Failed to get JWT token: ${error.message}`);
      console.error("   Cannot proceed with API approach. Exiting.");
      return;
    }
  }

  try {
    for (const [key, category] of Object.entries(MADEWELL_CATEGORIES)) {
      categoryCount++;
      console.log(
        `\n🏪 Category ${categoryCount}/${totalCategories}: ${category.name}`
      );

      // Step 1: Fetch product list from category
      const productList = await fetchMadewellProductList(
        category.url,
        minProductsPerCategory
      );

      console.log(`\n📦 Collected ${productList.length} product IDs`);
      console.log(`🔍 Now fetching details via API...\n`);

      // Step 2: Fetch details via API in batches
      const BATCH_SIZE = 10; // API supports multiple IDs
      let categoryProductsCount = 0;

      for (
        let batchStart = 0;
        batchStart < productList.length;
        batchStart += BATCH_SIZE
      ) {
        const batchEnd = Math.min(batchStart + BATCH_SIZE, productList.length);
        const batch = productList.slice(batchStart, batchEnd);

        console.log(
          `\n📦 Batch ${Math.floor(batchStart / BATCH_SIZE) + 1}/${Math.ceil(
            productList.length / BATCH_SIZE
          )}: Fetching ${batch.length} products via API...`
        );

        const productIds = batch.map((p) => p.id);

        // Fetch from API
        const apiProducts = await fetchProductDetailsFromAPI(
          productIds,
          jwtToken,
          allCookies
        );

        console.log(`   ✅ Received ${apiProducts.length} products from API`);

        // Parse and save
        for (const apiProduct of apiProducts) {
          const productId = apiProduct.id;

          // Skip duplicates
          if (processedProductIds.has(productId)) {
            console.log(`   ⏭️  Skipping duplicate: ${productId}`);
            continue;
          }

          // Fetch reviews for this product (async, don't wait too long)
          let reviewData = null;
          try {
            reviewData = await fetchProductReviews(productId);
          } catch (error) {
            // Reviews are optional, continue without them
          }

          const product = parseAPIProduct(apiProduct, reviewData);

          if (product && product.variants.length > 0) {
            // Write immediately to disk
            try {
              appendProductIncremental(inc, product);
              categoryProductsCount++;
              totalProducts++;
              processedProductIds.add(productId);

              const reviewInfo =
                reviewData && reviewData.review_count > 0
                  ? ` (⭐${reviewData.average_ratings.toFixed(1)} - ${
                      reviewData.review_count
                    } reviews)`
                  : "";

              console.log(
                `   ✅ ${product.name} (${product.variants.length} variants)${reviewInfo} [Written]`
              );
            } catch (writeError) {
              console.log(`   ❌ Error writing: ${writeError.message}`);
            }
          } else {
            console.log(`   ⚠️  Skipping ${productId} - no variants`);
          }
        }

        // Small delay between batches
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }

      console.log(
        `\n✅ Completed ${category.name}: ${categoryProductsCount} products written`
      );
    }
  } catch (error) {
    console.error(`\n❌ Error during crawling: ${error.message}`);
    console.error(error.stack);
  } finally {
    try {
      const files = finalizeIncrementalCatalog(inc);
      console.log(
        `\n📦 Catalog finalized. Total products written: ${totalProducts}`
      );
      console.log(`🔍 Total unique product IDs: ${processedProductIds.size}`);
      return {
        jsonPath: files.jsonPath,
        totalProductIds: totalProducts,
      };
    } catch (finalizeError) {
      console.error(`\n❌ Error finalizing: ${finalizeError.message}`);
      return {
        jsonPath: inc.jsonPath,
        totalProductIds: totalProducts,
      };
    }
  }
}

// ============================================================================
// MAIN ENTRY POINT
// ============================================================================

async function runMadewellCrawler(options = {}) {
  const { minProductsPerCategory = 1600 } = options;

  console.log("=".repeat(80));
  console.log("🚀 MADEWELL FAST API CRAWLER");
  console.log("=".repeat(80));
  console.log("✅ Launch browser ONCE to get JWT token");
  console.log("✅ Use Puppeteer for category listing");
  console.log("✅ Use API for ALL product details (NO clicking!)");
  console.log("⚡ 10-50x FASTER than old approach!");
  console.log("=".repeat(80));
  console.log(`🎯 Target: ${minProductsPerCategory} products per category\n`);

  try {
    const result = await madewellMain(minProductsPerCategory);

    if (result) {
      console.log("\n✅ Madewell crawling completed successfully!");
      console.log(`📁 Files generated: ${result.jsonPath}`);
      console.log(`📊 Total products processed: ${result.totalProductIds}`);
      return result;
    } else {
      console.log("\n❌ Madewell crawling failed");
      return false;
    }
  } catch (error) {
    console.error("\n💥 Madewell crawler error:", error.message);
    return false;
  }
}

// If run directly from command line
if (require.main === module) {
  const args = process.argv.slice(2);
  const options = {};

  for (let i = 0; i < args.length; i += 2) {
    const key = args[i];
    const value = args[i + 1];

    if (key === "--min-products" && value) {
      options.minProductsPerCategory = parseInt(value);
    }
  }

  runMadewellCrawler(options)
    .then((result) => {
      if (result) {
        console.log("\n🎉 Madewell crawler finished successfully!");
        process.exit(0);
      } else {
        console.log("\n❌ Madewell crawler failed");
        process.exit(1);
      }
    })
    .catch((error) => {
      console.error("💥 Unexpected error:", error);
      process.exit(1);
    });
}

module.exports = {
  runMadewellCrawler,
  madewellMain,
};
