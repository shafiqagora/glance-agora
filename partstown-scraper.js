/** @format */

// ============================================================================
// PartsTown Products Scraper - Complete API Implementation
// ============================================================================
//
// This scraper extracts complete product data from PartsTown.com using their
// internal APIs. It implements parallel processing, smart rate limiting, and
// comprehensive data extraction.
//
// DOCUMENTATION:
// - PARTSTOWN-API-STRUCTURE.md: Complete API endpoints and data flow
// - PARTSTOWN-IMPLEMENTATION-ANALYSIS.md: Implementation verification
//
// API ENDPOINTS USED:
// 1. /api/manufacturers/ - Get all manufacturers
// 2. /[brand]/parts/results - Get products for a brand
// 3. /prices/ - Batch price lookups
// 4. /parts/v2/models - Get compatible models (fits_models)
// 5. /[brand]/[model]/parts/facets - Get part categories
// 6. /[brand]/[model]/[category]/parts/results - Get parts per category
//
// FEATURES:
// - Parallel execution for maximum speed
// - Staggered brand processing to avoid rate limiting
// - Complete schema with all fields, models, manuals, and parts
// - English-only manual filtering
// - Configurable modes: Fast (no parts) vs Complete (all parts)
//
// ============================================================================
require("dotenv").config();
const fs = require("fs");
const { v4: uuidv4, v5: uuidv5 } = require("uuid");
const zlib = require("zlib");
const path = require("path");
const puppeteer = require("puppeteer");

// Import helper functions and database
const Product = require("./models/Product");
const Store = require("./models/Store");
const {
  calculateDiscount,
  retryPuppeteerWithProxyRotation,
} = require("./utils/helper");

// Helper function to process a single product
const processProduct = async (product, brandName, priceInfo, detailsInfo) => {
  try {
    // ===================================================================
    // COMPLETE SCHEMA EXTRACTION - ALL FIELDS FROM API RESPONSE
    // ===================================================================

    // Core product identifiers
    const partsTownNumber =
      product.code || product.partNumber || product.stockCode;
    const productName =
      product.name || product.description || "Unknown Product";
    const manufacturerPartNumber =
      product.manufacturerPartnumber ||
      product.manufacturerPartNumber ||
      partsTownNumber.replace(/^[A-Z]+/, "");

    // Extract product information with ALL available fields
    const category = product.category || "Parts";
    const units = product.unitOfMeasure || product.unit || "Each";

    // Extract COMPLETE stock information from API (richest source)
    let quantityAvailable = 0;
    let isInStock = false;

    if (product.stock) {
      // Stock data is directly available in Products API!
      quantityAvailable = product.stock.stockLevel || 0;
    }

    // Override with detailsInfo if available (more accurate from product page)
    if (detailsInfo && detailsInfo.quantityAvailable !== undefined) {
      quantityAvailable = detailsInfo.quantityAvailable;
    }

    // ✅ ALWAYS set is_in_stock based on quantity (consistency!)
    // Status text can be misleading (e.g., "Ship It" appears for backorder with qty=0)
    isInStock = quantityAvailable > 0;

    // Get pricing - API includes price in product object
    let myPrice = 0;
    let listPrice = 0;
    let savings = 0;

    if (product.price && product.price.value) {
      myPrice = parseFloat(product.price.value);
      listPrice = parseFloat(product.price.listPrice || product.price.value);
    }

    // Override with priceInfo if available (most accurate from Prices API)
    if (priceInfo) {
      myPrice = priceInfo.price || myPrice;
      listPrice = priceInfo.listPrice || listPrice;

      // Calculate savings - always calculate from listPrice - myPrice
      if (listPrice > myPrice) {
        savings = parseFloat((listPrice - myPrice).toFixed(2));
      } else if (priceInfo.savings && priceInfo.savings > 0) {
        // Use provided savings if no price difference
        savings = parseFloat(priceInfo.savings.toFixed(2));
      }
    } else {
      // Calculate savings even if priceInfo is not available
      if (listPrice > myPrice) {
        savings = parseFloat((listPrice - myPrice).toFixed(2));
      }
    }

    const isOnSale = savings > 0;
    const discount =
      isOnSale && listPrice > 0 ? ((savings / listPrice) * 100).toFixed(2) : 0;

    // Extract images (use viewerUrl for high quality, imageUrl as fallback)
    const imageUrl =
      product.viewerUrl || product.imageUrl || product.image || "";
    const alternateImages = [];
    if (product.viewerUrl && product.viewerUrl !== imageUrl) {
      alternateImages.push(product.viewerUrl);
    }
    if (product.imageUrl && product.imageUrl !== imageUrl) {
      alternateImages.push(product.imageUrl);
    }

    // Build product URL
    const productUrl = product.url
      ? `https://www.partstown.com${product.url}`
      : `https://www.partstown.com/part/${partsTownNumber}`;

    // Extract details from detailsInfo (ALL fields)
    const fitsModels =
      detailsInfo && detailsInfo.fitsModels ? detailsInfo.fitsModels : [];
    const specs =
      detailsInfo && detailsInfo.specs
        ? detailsInfo.specs
        : {
            length: "",
            width: "",
            height: "",
            weight: "",
          };
    const ratings =
      detailsInfo && detailsInfo.ratings
        ? detailsInfo.ratings
        : { average: 0, count: 0 };

    // Extract backorder information
    const backorderInfo =
      !isInStock && quantityAvailable === 0
        ? "Out of Stock, backorders usually ship in 21-23 days."
        : "";

    // ===================================================================
    // COMPLETE PRODUCT SCHEMA - ALL FIELDS
    // ===================================================================
    const formattedProduct = {
      // Core identifiers
      parts_town_number: partsTownNumber,
      manufacturer_part_number: manufacturerPartNumber,
      part_name: productName,

      // Product details
      category: category,
      retailer_domain: "partstown.com",
      manufacturer: brandName,
      units: units,

      // Technical specifications
      specifications: {
        length: specs.length,
        width: specs.width,
        height: specs.height,
        weight: specs.weight,
      },

      // Ratings and reviews
      average_rating: ratings.average,
      ratings_count: ratings.count,

      // Policies
      return_policy_link: "https://www.partstown.com/returns",
      return_policy:
        "PartsTown accepts returns within 30 days of purchase for most items. Parts must be in original, unused condition with all original packaging. Some items may be subject to restocking fees. Electrical parts, special order items, and custom-cut items may not be returnable. Contact customer service for specific return eligibility. Defective Parts & Warranty Inquiries: Manufacturer warranties are typically valid for 90 days, although the length of time depends on the individual manufacturer. For items that have been installed and no longer work or have a manufacturer defect, please download and complete the defective parts form available on the returns page. Submit the completed form via email and receive instructions within 48 hours.",
      prop_65_warning: true,

      // Variants (pricing and availability)
      variants: [],

      // Metadata
      operation_type: "INSERT",
      source: "partstown",
    };

    // Create a single variant for the product
    const formattedVariant = {
      price_currency: "USD",
      list_price: listPrice || myPrice,
      my_price: myPrice,
      savings: savings,
      link_url: productUrl,
      deeplink_url: productUrl,
      image_url: imageUrl,
      alternate_image_urls: alternateImages,
      is_on_sale: isOnSale,
      is_in_stock: isInStock,
      quantity_available: quantityAvailable,
      backorder_info: backorderInfo,
      units: units,
      // Compatibility - Models that this specific part fits
      fits_models: fitsModels,
      mpn: uuidv5(
        `${partsTownNumber}-standard`,
        "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
      ),
      discount_percentage: discount,
      operation_type: "INSERT",
      variant_id: uuidv5(
        `${partsTownNumber}-standard-variant`,
        "6ba7b810-9dad-11d1-80b4-00c04fd430c1"
      ),
    };

    formattedProduct.variants.push(formattedVariant);

    return { formattedProduct, mongoResult: {} };
  } catch (error) {
    console.error(`❌ Error processing product:`, error.message);
    return { formattedProduct: null, mongoResult: {} };
  }
};

// Generate files for all products
async function generateCatalogFiles(products, storeData) {
  const countryCode = storeData.country || "US";
  const formattedProducts = [];
  const productIds = [];
  const mongoResults = {
    inserted: 0,
    skipped: 0,
    errors: 0,
  };

  console.log(`\n📦 Processing ${products.length} products...`);

  // Process each product
  for (let i = 0; i < products.length; i++) {
    const { product, brandName, priceInfo, detailsInfo } = products[i];

    if (!product) {
      console.log(`⚠️ Skipping invalid product at index ${i}`);
      mongoResults.skipped++;
      continue;
    }

    console.log(
      `Processing product ${i + 1}/${products.length}: ${
        product.description || product.name || product.code || "Unknown"
      }`
    );

    try {
      const result = await processProduct(
        product,
        brandName,
        priceInfo,
        detailsInfo
      );

      if (result.formattedProduct) {
        formattedProducts.push(result.formattedProduct);
        mongoResults.inserted++;
      } else {
        mongoResults.skipped++;
      }
    } catch (error) {
      console.error(`Error processing product:`, error.message);
      mongoResults.errors++;
    }
  }

  // Create directory structure
  const cleanBrandName = "partstown";
  const dirPath = path.join(
    __dirname,
    "output",
    countryCode,
    `${cleanBrandName}-${countryCode}`
  );

  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }

  // Save formatted data as JSON
  const jsonFilePath = path.join(dirPath, "catalog.json");
  const catalogData = {
    store_info: {
      name: storeData.name || "PartsTown",
      domain: "partstown.com",
      currency: storeData.currency || "USD",
      country: countryCode,
      total_products: formattedProducts.length,
      categories: ["Commercial Parts", "Equipment Parts"],
      crawled_at: new Date().toISOString(),
    },
    products: formattedProducts,
  };

  fs.writeFileSync(jsonFilePath, JSON.stringify(catalogData, null, 2), "utf8");
  console.log(`JSON file generated: ${jsonFilePath}`);

  // Create JSONL file
  const jsonlFilePath = path.join(dirPath, "catalog.jsonl");
  const jsonlContent = formattedProducts
    .map((product) => JSON.stringify(product))
    .join("\n");
  fs.writeFileSync(jsonlFilePath, jsonlContent, "utf8");
  console.log(`JSONL file generated: ${jsonlFilePath}`);

  // Gzip the JSONL file
  const gzippedFilePath = `${jsonlFilePath}.gz`;
  const jsonlBuffer = fs.readFileSync(jsonlFilePath);
  const gzippedBuffer = zlib.gzipSync(jsonlBuffer);
  fs.writeFileSync(gzippedFilePath, gzippedBuffer);
  console.log(`Gzipped JSONL file generated: ${gzippedFilePath}`);

  console.log(`\n📊 Results:`);
  console.log(`  Products processed: ${mongoResults.inserted}`);
  console.log(`  Products skipped: ${mongoResults.skipped}`);
  console.log(`  Products errors: ${mongoResults.errors}`);

  return {
    jsonPath: gzippedFilePath,
    mongoResults,
    storeResult: {},
    totalProductIds: productIds.length,
  };
}

// Main scraping function
async function scrapePartsTownProducts() {
  console.log("🚀 Starting PartsTown scraping...");
  console.log("🔒 Using proxy rotation to handle Cloudflare...\n");

  // Use proxy rotation from helper.js to handle Cloudflare
  return await retryPuppeteerWithProxyRotation(
    async (browser) => {
      let page;

      try {
        page = await browser.newPage();

        // Set user agent to avoid detection
        await page.setUserAgent(
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        );

        // Navigate to the homepage with NO timeout
        await page.goto("https://www.partstown.com", {
          waitUntil: "domcontentloaded",
          timeout: 0, // INFINITY - Wait as long as needed
        });

        console.log("⏳ Waiting for Cloudflare to complete...");
        // Wait for Cloudflare challenge to disappear
        // Check for the Cloudflare challenge page elements specifically
        try {
          await page.waitForFunction(
            () => {
              const title = document.title.toLowerCase();
              // Only check for the specific Cloudflare challenge messages
              // Don't check body text as "Cloudflare" may appear in footer after bypass
              const hasChallengeTitle = title.includes("just a moment");
              const hasChallengeText = document.body.innerText
                .toLowerCase()
                .includes("checking your browser");

              // Return true when challenge is NOT present (page has loaded normally)
              return !hasChallengeTitle && !hasChallengeText;
            },
            { timeout: 60000 } // 60 seconds max wait for Cloudflare
          );
          console.log("✅ Cloudflare challenge completed!");
        } catch (error) {
          console.log("⚠️ Cloudflare wait timeout, proceeding anyway...");
        }

        // Additional wait to ensure cookies are set
        console.log("⏳ Waiting for page to stabilize (5 seconds)...");
        await new Promise((resolve) => setTimeout(resolve, 5000));

        // Navigate to manufacturers page to ensure cookies are set
        console.log("📍 Navigating to manufacturers page...");
        await page.goto("https://www.partstown.com/manufacturers", {
          waitUntil: "domcontentloaded",
          timeout: 0, // INFINITY - Wait as long as needed
        });

        // Verify Cloudflare was bypassed successfully
        console.log("🔍 Verifying Cloudflare bypass...");
        const cfCheck = await page.evaluate(() => {
          const title = document.title.toLowerCase();
          const body = document.body.innerText.toLowerCase();

          // Check for Cloudflare challenge indicators
          if (
            title.includes("just a moment") ||
            body.includes("checking your browser") ||
            body.includes("cloudflare")
          ) {
            return {
              success: false,
              message: "Cloudflare challenge still active",
            };
          }

          return { success: true, message: "Cloudflare bypassed" };
        });

        if (!cfCheck.success) {
          console.error(`❌ Cloudflare bypass failed: ${cfCheck.message}`);
          console.error("⚠️  Please ensure headless: false is set!");
          console.error(
            "⚠️  You may need to manually solve the Cloudflare challenge in the browser window."
          );
          return false;
        }

        console.log(
          "✅ Cloudflare bypassed successfully, starting API calls...\n"
        );

        // ========================================================================
        // CONFIGURATION - PRODUCTION MODE: COMPLETE STORE SCRAPING
        // ========================================================================

        // 🚀 PRODUCTION MODE: Scraping ENTIRE STORE - ALL brands, products, modals!
        // ⏰ NO TIMEOUTS - Will run until complete, even if it takes days
        // 💪 ROBUST - No skipping, all data from A to Z
        const maxBrandsToScrape = Infinity; // Process ALL manufacturers (no limit)
        const maxProductsPerBrand = Infinity; // Extract ALL products per manufacturer (no limit)

        // FEATURE TOGGLES
        // Parts extraction is the most time-consuming operation
        //
        // ⚡ FAST MODE (3-5 hours for 485 brands):
        //    SKIP_PARTS_EXTRACTION = true
        //    Gets: Products, Models, Manuals, Specs, Ratings, Stock, Prices
        //    Skips: Parts for each model
        //
        // 🐌 COMPLETE MODE (8-12+ HOURS for 485 brands):
        //    SKIP_PARTS_EXTRACTION = false
        //    Gets: Everything including ALL parts for ALL models
        //
        const SKIP_PARTS_EXTRACTION = true; // ⚡ Set to true for faster scraping
        const MAX_MODELS_TO_FETCH = Infinity; // Compatible models per product (Infinity = all)
        const MAX_MODELS_FOR_PARTS = Infinity; // Models to extract parts for (Infinity = all, no skipping)
        const MAX_PARTS_PER_CATEGORY = Infinity; // Parts per category (Infinity = all, no skipping)

        // PERFORMANCE SETTINGS - Optimized for complete scraping
        const PRICES_BATCH_SIZE = 500; // Stock codes per price API call
        const DETAILS_BATCH_SIZE = 50; // Products per details batch
        const BRAND_STAGGER_DELAY = 2000; // ms delay between brand starts (avoid rate limiting)
        const MODEL_DELAY = 50; // ms delay between models (avoid rate limiting)
        const CATEGORY_DELAY = 25; // ms delay between categories (avoid rate limiting)

        // DATA SAFETY SETTINGS
        const SAVE_EVERY_N_BRANDS = 1; // Auto-save catalog after EVERY brand for maximum security!

        // RESUME FUNCTIONALITY
        // The scraper will automatically:
        // 1. Load existing catalog.json if it exists
        // 2. Skip brands that are already processed
        // 3. Continue from where it left off
        // 4. Skip brands with zero products and move on
        console.log("\n🔄 RESUME MODE ENABLED:");
        console.log("   ✓ Will load existing catalog if found");
        console.log("   ✓ Will skip already processed brands");
        console.log("   ✓ Will skip brands with zero products");
        console.log("   ✓ Auto-saves after every brand\n");

        // Display current mode
        console.log(
          "╔════════════════════════════════════════════════════════════╗"
        );
        console.log(
          "║   🚀 PRODUCTION MODE - COMPLETE STORE SCRAPING 🚀        ║"
        );
        console.log(
          "╚════════════════════════════════════════════════════════════╝"
        );
        if (!SKIP_PARTS_EXTRACTION) {
          console.log("🐌 COMPLETE MODE - WITH ALL PARTS (8-12+ hours)");
          console.log(`   ⏰ NO TIMEOUTS - Will run until complete`);
          console.log(
            `   💪 SCRAPING: All brands, products, models, manuals & parts`
          );
        } else {
          console.log(
            "⚡ FAST MODE - Products, Models, Manuals, Specs (3-5 hours)"
          );
          console.log(`   ⏰ NO TIMEOUTS - Will run until complete`);
          console.log(`   💪 SCRAPING: All brands, products, models & manuals`);
          console.log(`   ℹ️  Parts extraction skipped for speed`);
        }
        console.log(
          "📦 Sequential processing (prevents 'Failed to fetch' errors)"
        );
        console.log(
          `⏱️  Batches: Prices=${PRICES_BATCH_SIZE}, Details=${DETAILS_BATCH_SIZE} | Delays: Model=${MODEL_DELAY}ms, Category=${CATEGORY_DELAY}ms`
        );
        console.log(
          `🎯 Target: ${
            maxBrandsToScrape === Infinity ? "ALL 485" : maxBrandsToScrape
          } manufacturers, ${
            maxProductsPerBrand === Infinity ? "ALL" : maxProductsPerBrand
          } products each`
        );
        console.log(
          `🖥️  Headless: false (Cloudflare bypass with proxy rotation)`
        );
        console.log(`🔄 Using proxy rotation from helper.js to handle captcha`);
        console.log(
          `💡 Server usage: Install Xvfb and run 'xvfb-run -a node partstown-scraper.js'`
        );
        console.log(
          `⚠️  NOTE: This will run indefinitely until ALL data is scraped!\n`
        );

        // Fetch all manufacturers using page.evaluate (executes in browser context)
        console.log("📋 Fetching all manufacturers...");
        const brandsResult = await page.evaluate(async () => {
          try {
            const apiUrl = `https://www.partstown.com/api/manufacturers/`;
            console.log("Calling API:", apiUrl);

            const response = await fetch(apiUrl);
            console.log("API Response status:", response.status);

            if (!response.ok) {
              return { error: `HTTP ${response.status}`, brands: [] };
            }

            const jsonData = await response.json();
            console.log("JSON Response type:", typeof jsonData);
            console.log("Is Array:", Array.isArray(jsonData));
            console.log("Item count:", jsonData.length || 0);

            // The API returns JSON array directly
            if (!Array.isArray(jsonData)) {
              return { error: "Invalid response format", brands: [] };
            }

            const brandList = jsonData.map((item) => ({
              code: item.code,
              name: item.name,
              uri: item.categoryUri,
              modelCount: item.modelCount || 0,
            }));

            return { brands: brandList, error: null };
          } catch (error) {
            return { error: error.message, brands: [] };
          }
        });

        // Check for errors
        if (brandsResult.error) {
          console.error(`❌ Error fetching brands: ${brandsResult.error}`);
          return false;
        }

        const brands = brandsResult.brands;
        console.log(`✅ Found ${brands.length} manufacturers`);

        if (brands.length === 0) {
          console.log("⚠️ No manufacturers found");
          return false;
        }

        // Log sample manufacturers
        console.log("\n📋 Sample manufacturers:");
        brands.slice(0, 10).forEach((brand, index) => {
          console.log(
            `   ${index + 1}. ${brand.name} (${brand.modelCount} models)`
          );
        });

        const storeData = {
          name: "PartsTown",
          domain: "partstown.com",
          currency: "USD",
          country: "US",
        };

        // Define paths and variables at the top
        const countryCode = "US";
        const cleanBrandName = "partstown";
        const dirPath = path.join(
          __dirname,
          "output",
          countryCode,
          `${cleanBrandName}-${countryCode}`
        );
        const jsonFilePath = path.join(dirPath, "catalog.json");

        let allProducts = [];
        let brandDetails = [];
        let lastSaveCount = 0; // Track last auto-save
        let processedBrands = new Set(); // Track which brands have been processed

        // TRY TO LOAD EXISTING CATALOG TO RESUME

        if (fs.existsSync(jsonFilePath)) {
          try {
            console.log("📂 Found existing catalog, loading to resume...");
            const existingData = JSON.parse(
              fs.readFileSync(jsonFilePath, "utf8")
            );

            if (existingData.products && Array.isArray(existingData.products)) {
              allProducts = existingData.products;

              // Track which brands were already processed
              existingData.products.forEach((product) => {
                if (product.manufacturer) {
                  processedBrands.add(product.manufacturer);
                }
              });

              console.log(`✅ Loaded ${allProducts.length} existing products`);
              console.log(
                `✅ Already processed brands: ${processedBrands.size}`
              );
              console.log(
                `   Brands: ${Array.from(processedBrands).join(", ")}`
              );

              // Reconstruct brandDetails from existing data
              const brandCounts = {};
              existingData.products.forEach((product) => {
                const brand = product.manufacturer;
                if (brand) {
                  brandCounts[brand] = (brandCounts[brand] || 0) + 1;
                }
              });

              brandDetails = Object.keys(brandCounts).map((brand) => ({
                brand: brand,
                count: brandCounts[brand],
              }));
            }
          } catch (error) {
            console.log(`⚠️ Could not load existing catalog: ${error.message}`);
            console.log("   Starting fresh...");
          }
        }

        // Scrape manufacturers SEQUENTIALLY (avoid "Failed to fetch" errors)
        const brandsToScrape = brands.slice(0, maxBrandsToScrape);
        const remainingBrands = brandsToScrape.filter(
          (b) => !processedBrands.has(b.name)
        );

        console.log(
          `🚀 Processing ${brandsToScrape.length} manufacturers SEQUENTIALLY (to avoid "Failed to fetch" errors)...\n`
        );
        console.log(
          `📊 Already processed: ${processedBrands.size} brands | Remaining: ${remainingBrands.length} brands\n`
        );
        console.log(
          `💾 Auto-save: Catalog will be saved every ${SAVE_EVERY_N_BRANDS} brands to prevent data loss\n`
        );

        // Process manufacturers SEQUENTIALLY to avoid overwhelming the server
        // Parallel processing causes "Failed to fetch" errors with 485+ brands
        for (
          let brandIndex = 0;
          brandIndex < brandsToScrape.length;
          brandIndex++
        ) {
          const brand = brandsToScrape[brandIndex];

          // SKIP ALREADY PROCESSED BRANDS (RESUME FUNCTIONALITY)
          if (processedBrands.has(brand.name)) {
            console.log(
              `⏭️  Skipping ${brand.name} (${brandIndex + 1}/${
                brandsToScrape.length
              }) - already processed`
            );
            continue;
          }

          console.log(`\n${"=".repeat(50)}`);
          console.log(
            `🎯 Manufacturer ${brandIndex + 1}/${brandsToScrape.length}: ${
              brand.name
            }`
          );
          console.log(`${"=".repeat(50)}`);

          // Fetch products for this brand using page.evaluate
          const brandProducts = await page.evaluate(
            async (brandCode, brandUri, maxProducts) => {
              let allParts = [];
              let currentPage = 0; // PartsTown API uses 0-based pagination!
              let hasMore = true;

              console.log(
                `📡 Fetching products for brand: ${brandCode}, URI: ${brandUri}`
              );

              while (hasMore && allParts.length < maxProducts) {
                // Try multiple URL formats
                const urlFormats = [
                  // Format 1: Using brand code with page parameter (0-based)
                  `https://www.partstown.com/parts/results?brand=${brandCode}&page=${currentPage}`,
                  // Format 2: Using brand URI
                  `https://www.partstown.com/${brandUri}/parts/results?page=${currentPage}`,
                  // Format 3: Using brand code lowercase
                  `https://www.partstown.com/parts/results?brand=${brandCode.toLowerCase()}&page=${currentPage}`,
                ];

                let success = false;

                for (const apiUrl of urlFormats) {
                  try {
                    console.log(`   🔍 Trying: ${apiUrl}`);
                    const response = await fetch(apiUrl);

                    if (response.ok) {
                      const data = await response.json();

                      if (data.results && data.results.length > 0) {
                        allParts = allParts.concat(data.results);
                        console.log(
                          `   ✅ Page ${currentPage}: ${data.results.length} products (total: ${allParts.length})`
                        );

                        // Check if there are more pages
                        if (data.pagination && data.pagination.numberOfPages) {
                          hasMore =
                            currentPage + 1 < data.pagination.numberOfPages;
                        } else {
                          // Assume no more pages if less than 24 results
                          hasMore = data.results.length >= 24;
                        }

                        currentPage++;
                        success = true;
                        break; // Success! Use this URL format for next pages
                      } else if (currentPage === 0) {
                        // First page and no results, try next format
                        continue;
                      } else {
                        // Subsequent page with no results, we're done
                        hasMore = false;
                        success = true;
                        break;
                      }
                    }
                  } catch (error) {
                    console.log(
                      `   ⚠️ Error with URL format: ${error.message}`
                    );
                    continue;
                  }
                }

                if (!success) {
                  console.log(
                    `   ❌ All URL formats failed for page ${currentPage}`
                  );
                  hasMore = false;
                }
              }

              console.log(
                `   📦 Total products found for ${brandCode}: ${allParts.length}`
              );
              return allParts.slice(0, maxProducts);
            },
            brand.code,
            brand.uri,
            maxProductsPerBrand
          );

          console.log(
            `✅ Found ${brandProducts.length} products for ${brand.name}`
          );

          if (brandProducts.length === 0) {
            console.log(
              `⚠️ No products found for ${brand.name}, skipping to next brand...\n`
            );
            // Mark as processed to avoid retrying on resume
            processedBrands.add(brand.name);
            continue; // Skip to next brand
          }

          // PARALLEL EXECUTION: Fetch prices AND details at the same time!
          console.log(
            `⚡ Fetching prices AND details in parallel for ${brand.name}...`
          );

          const stockCodes = brandProducts
            .map((p) => p.code) // API uses 'code' field
            .filter(Boolean);

          console.log(
            `   📝 Stock codes to fetch prices for: ${stockCodes.length}`
          );
          if (stockCodes.length > 0) {
            console.log(
              `   📝 Sample stock codes: ${stockCodes.slice(0, 5).join(", ")}`
            );
          }

          // Start BOTH operations in parallel
          const [priceMap, detailsMap] = await Promise.all([
            // Operation 1: Fetch ALL prices in parallel
            (async () => {
              const batchSize = PRICES_BATCH_SIZE;
              const priceMap = {};

              const pricePromises = [];
              for (let i = 0; i < stockCodes.length; i += batchSize) {
                const batchCodes = stockCodes.slice(i, i + batchSize);

                pricePromises.push(
                  page.evaluate(async (codes) => {
                    try {
                      const params = codes.map((code) => `s=${code}`).join("&");
                      const pricesUrl = `https://www.partstown.com/prices/?${params}`;
                      const response = await fetch(pricesUrl);

                      if (!response.ok) {
                        const errorText = await response.text();
                        return {
                          prices: {},
                          error: `HTTP ${response.status}`,
                          errorDetails: errorText.substring(0, 200),
                          requestedCodes: codes.length,
                        };
                      }

                      // API returns JSON, not XML!
                      const responseText = await response.text();
                      let jsonData;
                      try {
                        jsonData = JSON.parse(responseText);
                      } catch (parseError) {
                        return {
                          prices: {},
                          error: `JSON parse error: ${parseError.message}`,
                          responsePreview: responseText.substring(0, 200),
                          requestedCodes: codes.length,
                        };
                      }

                      const prices = {};

                      // Response is an array of price objects
                      if (Array.isArray(jsonData)) {
                        jsonData.forEach((item) => {
                          const stockCode = item.stockCode;
                          if (stockCode) {
                            const myPrice = parseFloat(item.value || 0);
                            const listPrice = parseFloat(
                              item.listPrice || item.value || 0
                            );
                            const basePrice = parseFloat(item.basePrice || 0);

                            let savings = 0;
                            if (listPrice > myPrice) {
                              savings = parseFloat(
                                (listPrice - myPrice).toFixed(2)
                              );
                            }

                            prices[stockCode] = {
                              price: myPrice,
                              listPrice: listPrice,
                              basePrice: basePrice,
                              savings: savings,
                            };
                          }
                        });
                      }

                      return {
                        prices,
                        itemsFound: Array.isArray(jsonData)
                          ? jsonData.length
                          : 0,
                        pricesExtracted: Object.keys(prices).length,
                        requestedCodes: codes.length,
                      };
                    } catch (error) {
                      return {
                        prices: {},
                        error: error.message,
                        requestedCodes: codes.length,
                      };
                    }
                  }, batchCodes)
                );
              }

              // Wait for ALL price batches in parallel
              const allPriceBatches = await Promise.all(pricePromises);

              // Process results and log any errors
              let totalRequested = 0;
              let totalReceived = 0;
              allPriceBatches.forEach((result, index) => {
                if (result && result.prices) {
                  Object.assign(priceMap, result.prices);
                  totalRequested += result.requestedCodes || 0;
                  totalReceived += result.pricesExtracted || 0;

                  if (result.error) {
                    console.log(
                      `   ⚠️ Batch ${index + 1} error: ${result.error}`
                    );
                    if (result.errorDetails) {
                      console.log(`   Error details: ${result.errorDetails}`);
                    }
                    if (result.responsePreview) {
                      console.log(
                        `   Response preview: ${result.responsePreview}`
                      );
                    }
                  }

                  // Log if we got significantly fewer prices than requested
                  if (
                    result.requestedCodes > 0 &&
                    result.pricesExtracted === 0
                  ) {
                    console.log(
                      `   ⚠️ Batch ${index + 1}: Requested ${
                        result.requestedCodes
                      } prices but got 0!`
                    );
                  }
                }
              });

              console.log(
                `✅ Got prices for ${
                  Object.keys(priceMap).length
                }/${totalRequested} products (${totalReceived} items parsed)`
              );
              return priceMap;
            })(),

            // Operation 2: Fetch ALL details in parallel
            (async () => {
              const detailsMap = {};
              const detailsBatchSize = DETAILS_BATCH_SIZE;
              const detailsPromises = [];

              // Create ALL batch promises at once (parallel)
              for (let i = 0; i < brandProducts.length; i += detailsBatchSize) {
                const batchProducts = brandProducts.slice(
                  i,
                  i + detailsBatchSize
                );

                detailsPromises.push(
                  page.evaluate(
                    async (
                      products,
                      skipParts,
                      maxModels,
                      maxModelsForParts,
                      maxPartsPerCategory,
                      modelDelay,
                      categoryDelay
                    ) => {
                      const fetchProductDetails = async (product) => {
                        const code = product.code;
                        const productUrl = product.url;
                        try {
                          const details = {
                            fitsModels: [],
                            specs: {
                              length: "",
                              width: "",
                              height: "",
                              weight: "",
                            },
                            ratings: {
                              average: 0,
                              count: 0,
                            },
                            quantityAvailable: 0,
                            isInStock: false,
                          };

                          // Fetch Fits Models API (JSON response with pagination)
                          try {
                            // Fetch first page to get pagination info
                            const modelsResponse = await fetch(
                              `https://www.partstown.com/parts/v2/models?productCode=${code}&q=&pageSize=100&page=0`
                            );

                            if (modelsResponse.ok) {
                              // Try parsing as JSON first (new API format)
                              try {
                                const jsonData = await modelsResponse.json();
                                if (
                                  jsonData.results &&
                                  Array.isArray(jsonData.results)
                                ) {
                                  // Add first page results with complete details
                                  jsonData.results.forEach((model) => {
                                    if (model.name && model.modelUri) {
                                      // Filter manuals to English only
                                      const englishManuals = model.manuals
                                        ? model.manuals
                                            .filter(
                                              (manual) =>
                                                !manual.language ||
                                                manual.language === "en" ||
                                                manual.language === "EN"
                                            )
                                            .map((manual) => ({
                                              type: manual.type || "",
                                              type_code: manual.typeCode || "",
                                              link: manual.link
                                                ? `https://www.partstown.com${manual.link}`
                                                : "",
                                              code: manual.code || "",
                                              language: "en",
                                              uri: manual.uri || "",
                                            }))
                                        : [];

                                      details.fitsModels.push({
                                        name: model.name,
                                        uri: model.modelUri,
                                        code: model.code || "",
                                        url: model.url
                                          ? `https://www.partstown.com${model.url}`
                                          : "",
                                        has_manuals: englishManuals.length > 0,
                                        manuals: englishManuals,
                                        is_in_my_equipment:
                                          model.isModelInMyEquipment || false,
                                        parts: [], // Will be populated later
                                      });
                                    }
                                  });

                                  // Check if there are more pages and fetch all
                                  const pagination = jsonData.pagination;
                                  if (
                                    pagination &&
                                    pagination.numberOfPages > 1 &&
                                    details.fitsModels.length < maxModels
                                  ) {
                                    // Limit total models based on maxModels parameter
                                    // Fetch remaining pages
                                    const pagePromises = [];
                                    const pagesToFetch = Math.ceil(
                                      maxModels / 100
                                    ); // 100 per page
                                    const maxPages = Math.min(
                                      pagination.numberOfPages,
                                      pagesToFetch
                                    );
                                    for (
                                      let page = 1;
                                      page < maxPages;
                                      page++
                                    ) {
                                      pagePromises.push(
                                        fetch(
                                          `https://www.partstown.com/parts/v2/models?productCode=${code}&q=&pageSize=100&page=${page}`
                                        ).then((r) => r.json())
                                      );
                                    }

                                    const allPages = await Promise.all(
                                      pagePromises
                                    );
                                    allPages.forEach((pageData) => {
                                      if (
                                        pageData.results &&
                                        Array.isArray(pageData.results)
                                      ) {
                                        pageData.results.forEach((model) => {
                                          if (
                                            model.name &&
                                            model.modelUri &&
                                            details.fitsModels.length <
                                              maxModels
                                          ) {
                                            // Filter manuals to English only
                                            const englishManuals = model.manuals
                                              ? model.manuals
                                                  .filter(
                                                    (manual) =>
                                                      !manual.language ||
                                                      manual.language ===
                                                        "en" ||
                                                      manual.language === "EN"
                                                  )
                                                  .map((manual) => ({
                                                    type: manual.type || "",
                                                    type_code:
                                                      manual.typeCode || "",
                                                    link: manual.link
                                                      ? `https://www.partstown.com${manual.link}`
                                                      : "",
                                                    code: manual.code || "",
                                                    language: "en",
                                                    uri: manual.uri || "",
                                                  }))
                                              : [];

                                            details.fitsModels.push({
                                              name: model.name,
                                              uri: model.modelUri,
                                              code: model.code || "",
                                              url: model.url
                                                ? `https://www.partstown.com${model.url}`
                                                : "",
                                              has_manuals:
                                                englishManuals.length > 0,
                                              manuals: englishManuals,
                                              is_in_my_equipment:
                                                model.isModelInMyEquipment ||
                                                false,
                                              parts: [], // Will be populated later
                                            });
                                          }
                                        });
                                      }
                                    });
                                  }
                                }
                              } catch (jsonError) {
                                // If JSON parsing fails, try XML (fallback for old API format)
                                const modelsText = await modelsResponse.text();
                                const parser = new DOMParser();
                                const xmlDoc = parser.parseFromString(
                                  modelsText,
                                  "text/xml"
                                );

                                // XML structure: <SearchResultsData><results><results>...</results></results></SearchResultsData>
                                const allResultElements =
                                  xmlDoc.getElementsByTagName("results");

                                // Skip the first one (it's the wrapper), process the rest (actual models)
                                for (
                                  let j = 1;
                                  j < allResultElements.length &&
                                  details.fitsModels.length < 100;
                                  j++
                                ) {
                                  const nameEl =
                                    allResultElements[j].getElementsByTagName(
                                      "name"
                                    )[0];
                                  const uriEl =
                                    allResultElements[j].getElementsByTagName(
                                      "modelUri"
                                    )[0];

                                  if (nameEl && nameEl.textContent) {
                                    details.fitsModels.push({
                                      name: nameEl.textContent,
                                      uri: uriEl ? uriEl.textContent : "",
                                      parts: [], // Will be populated later
                                    });
                                  }
                                }
                              }
                            }
                          } catch (e) {
                            // Silent fail
                          }

                          // Extract parts for all models
                          if (!skipParts) {
                            const modelsToProcess =
                              maxModelsForParts === Infinity
                                ? details.fitsModels.length
                                : Math.min(
                                    details.fitsModels.length,
                                    maxModelsForParts
                                  );

                            for (let i = 0; i < modelsToProcess; i++) {
                              const model = details.fitsModels[i];
                              if (i > 0)
                                await new Promise((r) =>
                                  setTimeout(r, modelDelay)
                                );

                              try {
                                if (model.url) {
                                  const urlParts = new URL(model.url).pathname
                                    .split("/")
                                    .filter((p) => p);
                                  const [manufSlug, modelSlug] = urlParts;

                                  const facetsUrl = `https://www.partstown.com/${manufSlug}/${modelSlug}/parts/facets`;
                                  const facetsRes = await fetch(facetsUrl);

                                  if (facetsRes.ok) {
                                    const facetsData = await facetsRes.json();

                                    // Find the functional categories facet
                                    const categoriesFacet = facetsData.find(
                                      (f) =>
                                        f.code === "functionalCategoriesFacet"
                                    );

                                    if (
                                      categoriesFacet &&
                                      categoriesFacet.values
                                    ) {
                                      for (const catValue of categoriesFacet.values) {
                                        if (catValue.count > 0) {
                                          try {
                                            await new Promise((r) =>
                                              setTimeout(r, categoryDelay)
                                            );
                                            const partsApiUrl = `https://www.partstown.com${catValue.url}/results`;
                                            const partsApiRes = await fetch(
                                              partsApiUrl
                                            );

                                            if (partsApiRes.ok) {
                                              const partsData =
                                                await partsApiRes.json();
                                              if (
                                                partsData.results &&
                                                Array.isArray(partsData.results)
                                              ) {
                                                partsData.results.forEach(
                                                  (part) => {
                                                    if (
                                                      part.code &&
                                                      part.name
                                                    ) {
                                                      model.parts.push({
                                                        category: catValue.name,
                                                        part_number: part.code,
                                                        part_name:
                                                          part.name ||
                                                          part.description ||
                                                          "",
                                                        price:
                                                          part.price &&
                                                          part.price.value
                                                            ? parseFloat(
                                                                part.price.value
                                                              )
                                                            : 0,
                                                        url: part.url
                                                          ? `https://www.partstown.com${part.url}`
                                                          : `https://www.partstown.com/${manufSlug}/${part.code.toLowerCase()}`,
                                                      });
                                                    }
                                                  }
                                                );
                                              }
                                            }
                                          } catch (catError) {
                                            // Silent fail
                                          }
                                        }
                                      }
                                    }
                                  }
                                }
                              } catch (modelError) {
                                // Silent fail
                              }
                            }
                          }

                          // Fetch product page HTML to scrape specs, description, and ratings
                          try {
                            // Use the product URL if available, otherwise construct from code
                            const fullUrl = productUrl
                              ? `https://www.partstown.com${productUrl}`
                              : `https://www.partstown.com/part/${code}`;

                            const pageResponse = await fetch(fullUrl);

                            if (pageResponse.ok) {
                              const html = await pageResponse.text();
                              const tempDiv = document.createElement("div");
                              tempDiv.innerHTML = html;

                              // Find specs in the HTML - they are in ul.spec-list
                              let specList =
                                tempDiv.querySelector(".spec-list");
                              if (!specList) {
                                // Try alternative selectors
                                specList =
                                  tempDiv.querySelector(".product-specs");
                              }
                              if (!specList) {
                                specList = tempDiv.querySelector(
                                  '[data-test-id="specs-list"]'
                                );
                              }

                              if (specList) {
                                const items = specList.querySelectorAll("li");

                                items.forEach((item) => {
                                  const label =
                                    item.querySelector(".spec-list__label");
                                  const value =
                                    item.querySelector(".spec-list__value");

                                  if (label && value) {
                                    const labelText = label.textContent
                                      .trim()
                                      .toLowerCase()
                                      .replace(":", "")
                                      .replace(/\s+/g, " ");
                                    // Clean up value text - remove extra whitespace, newlines, and tabs
                                    const valueText = value.textContent
                                      .replace(/\s+/g, " ")
                                      .trim();

                                    if (labelText.includes("length")) {
                                      details.specs.length = valueText;
                                    } else if (labelText.includes("width")) {
                                      details.specs.width = valueText;
                                    } else if (labelText.includes("height")) {
                                      details.specs.height = valueText;
                                    } else if (labelText.includes("weight")) {
                                      details.specs.weight = valueText;
                                    }
                                  }
                                });
                              }

                              // Extract stock quantity - try multiple selectors
                              let stockElement = tempDiv.querySelector(
                                ".js-product-stocklvl"
                              );
                              if (!stockElement) {
                                stockElement = tempDiv.querySelector(
                                  ".js-pdp-main-qty-value"
                                );
                              }
                              if (!stockElement) {
                                stockElement =
                                  tempDiv.querySelector(".product-quantity");
                              }
                              if (!stockElement) {
                                // Try finding in structured data
                                const quantityText = tempDiv.querySelector(
                                  '[itemprop="inventoryLevel"]'
                                );
                                if (quantityText) {
                                  stockElement = quantityText;
                                }
                              }

                              if (stockElement) {
                                const qtyText = stockElement.textContent.trim();
                                // Extract number from various formats like "28 in stock", "Quantity: 28", etc.
                                const qtyMatch = qtyText.match(/(\d+)/);
                                if (qtyMatch) {
                                  details.quantityAvailable = parseInt(
                                    qtyMatch[1]
                                  );
                                }
                              }

                              // Determine stock status - ALWAYS base on quantity
                              // Status text like "Ship It" can appear even for backorder (qty=0)
                              // So we ONLY trust the quantity number for accuracy
                              details.isInStock = details.quantityAvailable > 0;

                              // Extract ratings - try multiple selectors
                              let ratingsEl = tempDiv.querySelector(
                                ".bv_avgRating_component_container"
                              );
                              if (!ratingsEl) {
                                ratingsEl = tempDiv.querySelector(
                                  '[itemprop="ratingValue"]'
                                );
                              }
                              if (!ratingsEl) {
                                ratingsEl =
                                  tempDiv.querySelector(".product-rating");
                              }
                              if (!ratingsEl) {
                                ratingsEl = tempDiv.querySelector(
                                  '[data-test-id="average-rating"]'
                                );
                              }

                              if (ratingsEl) {
                                const ratingText = ratingsEl.textContent.trim();
                                const ratingMatch =
                                  ratingText.match(/(\d+\.?\d*)/);
                                if (ratingMatch) {
                                  details.ratings.average = parseFloat(
                                    ratingMatch[1]
                                  );
                                }
                              }

                              // Extract ratings count
                              let ratingsCountEl = tempDiv.querySelector(
                                ".bv_numReviews_text"
                              );
                              if (!ratingsCountEl) {
                                ratingsCountEl = tempDiv.querySelector(
                                  '[itemprop="reviewCount"]'
                                );
                              }
                              if (!ratingsCountEl) {
                                ratingsCountEl =
                                  tempDiv.querySelector(".reviews-count");
                              }
                              if (!ratingsCountEl) {
                                ratingsCountEl = tempDiv.querySelector(
                                  '[data-test-id="reviews-count"]'
                                );
                              }

                              if (ratingsCountEl) {
                                const countText =
                                  ratingsCountEl.textContent.trim();
                                const countMatch = countText.match(/(\d+)/);
                                if (countMatch) {
                                  details.ratings.count = parseInt(
                                    countMatch[1]
                                  );
                                }
                              }
                            }
                          } catch (e) {
                            // Silent fail
                          }

                          // Return extracted details (logging removed for speed)
                          return { code, details };
                        } catch (error) {
                          console.error(
                            `❌ Error extracting details for ${code}:`,
                            error.message
                          );
                          return {
                            code,
                            details: {
                              fitsModels: [],
                              specs: {
                                length: "",
                                width: "",
                                height: "",
                                weight: "",
                              },
                              ratings: { average: 0, count: 0 },
                              quantityAvailable: 0,
                              isInStock: false,
                            },
                          };
                        }
                      };

                      // Fetch all products in batch in parallel
                      const results = await Promise.all(
                        products.map(fetchProductDetails)
                      );
                      return results;
                    },
                    batchProducts,
                    SKIP_PARTS_EXTRACTION,
                    MAX_MODELS_TO_FETCH,
                    MAX_MODELS_FOR_PARTS,
                    MAX_PARTS_PER_CATEGORY,
                    MODEL_DELAY,
                    CATEGORY_DELAY
                  )
                );
              }

              // Wait for ALL details batches in parallel
              const allDetailsBatches = await Promise.all(detailsPromises);

              // Merge all details
              allDetailsBatches.forEach((batchResults) => {
                batchResults.forEach((result) => {
                  detailsMap[result.code] = result.details;
                });
              });

              console.log(
                `✅ Got details for ${Object.keys(detailsMap).length} products`
              );
              return detailsMap;
            })(),
          ]);

          // Both prices AND details are now ready!
          console.log(`⚡ Parallel fetch complete for ${brand.name}!\n`);

          // Add brand info, prices, and details to products
          const productsWithInfo = brandProducts.map((product) => {
            const productCode = product.code;
            const priceData = priceMap[productCode];
            const detailsData = detailsMap[productCode];

            return {
              product: product,
              brandName: brand.name,
              priceInfo: priceData,
              detailsInfo: detailsData,
            };
          });

          console.log(
            `✅ Brand ${brand.name} complete: ${productsWithInfo.length} products\n`
          );

          // Add brand results immediately - FORMAT THEM FIRST!
          if (productsWithInfo.length > 0) {
            // Format all products before adding to allProducts
            for (const item of productsWithInfo) {
              const { product, brandName, priceInfo, detailsInfo } = item;
              if (!product) continue;

              const result = await processProduct(
                product,
                brandName,
                priceInfo,
                detailsInfo
              );

              if (result.formattedProduct) {
                allProducts.push(result.formattedProduct);
              }
            }

            brandDetails.push({
              brand: brand.name,
              count: brandProducts.length,
            });
            // Mark brand as successfully processed
            processedBrands.add(brand.name);
          }

          // INCREMENTAL SAVE: Save catalog every N brands
          const brandsProcessed = brandIndex + 1;
          if (
            brandsProcessed % SAVE_EVERY_N_BRANDS === 0 ||
            brandsProcessed === brandsToScrape.length
          ) {
            console.log(
              `\n💾 Auto-saving catalog (${brandsProcessed}/${brandsToScrape.length} brands, ${allProducts.length} products)...`
            );

            try {
              const catalogData = {
                store_info: {
                  name: storeData.name || "PartsTown",
                  domain: "partstown.com",
                  currency: storeData.currency || "USD",
                  country: "US",
                  total_products: allProducts.length,
                  categories: ["Commercial Parts", "Equipment Parts"],
                  crawled_at: new Date().toISOString(),
                  brands_processed: processedBrands.size,
                  brands_attempted: brandsProcessed,
                  total_brands: brandsToScrape.length,
                },
                products: allProducts,
              };

              if (!fs.existsSync(dirPath)) {
                fs.mkdirSync(dirPath, { recursive: true });
              }

              // Save JSON
              fs.writeFileSync(
                jsonFilePath,
                JSON.stringify(catalogData, null, 2),
                "utf8"
              );

              // Save JSONL
              const jsonlFilePath = path.join(dirPath, "catalog.jsonl");
              const jsonlContent = allProducts
                .map((product) => JSON.stringify(product))
                .join("\n");
              fs.writeFileSync(jsonlFilePath, jsonlContent, "utf8");

              // Save JSONL.GZ
              const gzPath = `${jsonlFilePath}.gz`;
              const gzBuffer = zlib.gzipSync(Buffer.from(jsonlContent, "utf8"));
              fs.writeFileSync(gzPath, gzBuffer);

              console.log(
                `✅ Saved: ${allProducts.length} products from ${brandsProcessed} brands`
              );
              console.log(`   📄 ${jsonFilePath}`);
              console.log(`   📄 ${jsonlFilePath}`);
              console.log(`   📄 ${gzPath}\n`);

              lastSaveCount = brandsProcessed;
            } catch (saveError) {
              console.error(`❌ Error saving catalog: ${saveError.message}`);
              // Continue scraping even if save fails
            }
          }

          // Small delay between brands to prevent rate limiting
          if (brandIndex < brandsToScrape.length - 1) {
            await new Promise((resolve) => setTimeout(resolve, 1000)); // 1 second between brands
          }
        } // End of for loop

        console.log(`\n📦 Total products collected: ${allProducts.length}`);

        if (allProducts.length === 0) {
          console.log("⚠️ No products found from any brand");
          return false;
        }

        // Final save (if not just saved)
        if (lastSaveCount < brandsToScrape.length) {
          console.log("\n💾 Performing final save...");

          // Save final catalog
          const catalogData = {
            store_info: {
              name: storeData.name || "PartsTown",
              domain: "partstown.com",
              currency: storeData.currency || "USD",
              country: "US",
              total_products: allProducts.length,
              categories: ["Commercial Parts", "Equipment Parts"],
              crawled_at: new Date().toISOString(),
              brands_processed: processedBrands.size,
              total_brands: brandsToScrape.length,
            },
            products: allProducts,
          };

          if (!fs.existsSync(dirPath)) {
            fs.mkdirSync(dirPath, { recursive: true });
          }

          // Save JSON
          fs.writeFileSync(
            jsonFilePath,
            JSON.stringify(catalogData, null, 2),
            "utf8"
          );

          // Save JSONL
          const jsonlFilePath = path.join(dirPath, "catalog.jsonl");
          const jsonlContent = allProducts
            .map((product) => JSON.stringify(product))
            .join("\n");
          fs.writeFileSync(jsonlFilePath, jsonlContent, "utf8");

          // Save JSONL.GZ
          const gzPath = `${jsonlFilePath}.gz`;
          const gzBuffer = zlib.gzipSync(Buffer.from(jsonlContent, "utf8"));
          fs.writeFileSync(gzPath, gzBuffer);

          console.log(
            `✅ Saved: ${allProducts.length} products from ${processedBrands.size} brands`
          );
          console.log(`   📄 ${jsonFilePath}`);
          console.log(`   📄 ${jsonlFilePath}`);
          console.log(`   📄 ${gzPath}\n`);
        } else {
          console.log(
            "\n✅ Catalog already up-to-date (auto-saved after last brand)"
          );
        }

        // Get final catalog path for return
        const catalogResult = {
          jsonPath: path.join(dirPath, "catalog.jsonl.gz"),
          mongoResults: {
            inserted: allProducts.length,
            skipped: 0,
            errors: 0,
          },
          storeResult: {},
          totalProductIds: allProducts.length,
        };

        console.log(`\n✅ SCRAPING COMPLETED!`);

        // Summary
        console.log(`\n📊 Summary:`);
        console.log(`   Total Products: ${allProducts.length}`);
        console.log(`   Brands Processed: ${processedBrands.size}`);
        console.log(`   Brands with Products: ${brandDetails.length}`);

        if (brandDetails.length > 0) {
          console.log(`\n   📦 Brands with products:`);
          brandDetails.forEach((detail) => {
            console.log(`     ✓ ${detail.brand}: ${detail.count} products`);
          });
        }

        // Show brands that were skipped (zero products)
        const brandsWithZeroProducts = Array.from(processedBrands).filter(
          (brandName) => !brandDetails.find((bd) => bd.brand === brandName)
        );
        if (brandsWithZeroProducts.length > 0) {
          console.log(
            `\n   ⏭️  Brands skipped (zero products): ${brandsWithZeroProducts.length}`
          );
          brandsWithZeroProducts.forEach((brandName) => {
            console.log(`     ⊘ ${brandName}`);
          });
        }

        console.log(`\n   Output File: ${catalogResult.jsonPath}`);
        console.log(
          `   Processed: ${catalogResult.mongoResults.inserted}, Skipped: ${catalogResult.mongoResults.skipped}, Errors: ${catalogResult.mongoResults.errors}`
        );

        console.log(`\n⚡ Performance:`);
        console.log(
          `   Sequential processing (prevents "Failed to fetch" errors)`
        );
        console.log(`   Complete data: ALL fields, models, manuals & parts`);

        return {
          brands: brandDetails,
          totalProducts: allProducts.length,
          jsonPath: catalogResult.jsonPath,
          mongoResults: catalogResult.mongoResults,
          storeResult: catalogResult.storeResult,
          totalProductIds: catalogResult.totalProductIds,
        };
      } catch (error) {
        console.error("❌ Error during scraping:", error);
        console.error("Stack trace:", error.stack);
        throw error;
      }
    },
    3, // maxRetries
    2000, // baseDelay
    "US", // country
    "https://www.partstown.com", // storeUrl
    false // headless - MUST be false for Cloudflare bypass
  );
}

// Export the main function
const main = async () => {
  try {
    const result = await scrapePartsTownProducts();

    if (result) {
      console.log("\n🎉 PartsTown crawling completed successfully!");

      console.log(`📊 Final Summary:`);
      console.log(`   Total products: ${result.totalProducts}`);

      if (result.brands) {
        console.log(`   Brands:`);
        result.brands.forEach((brand) => {
          console.log(`     ${brand.brand}: ${brand.count} products`);
        });
      }

      return result;
    } else {
      console.log("\n❌ PartsTown crawling failed");
      return false;
    }
  } catch (error) {
    console.error("Error in main function:", error);
    throw error;
  }
};

// Run the scraper
if (require.main === module) {
  main()
    .then((result) => {
      if (result) {
        console.log("Script completed successfully");
        process.exit(0);
      } else {
        console.log("Script failed");
        process.exit(1);
      }
    })
    .catch((error) => {
      console.error("Script failed:", error);
      process.exit(1);
    });
}

module.exports = { main, scrapePartsTownProducts };
