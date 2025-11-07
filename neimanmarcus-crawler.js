/** @format */
/**
 * Neiman Marcus Crawler - Complete crawler with proxy rotation
 */

require("dotenv").config();
const axios = require("axios");
const { v5: uuidv5 } = require("uuid");
const fs = require("fs");
const zlib = require("zlib");
const path = require("path");
const puppeteer = require("puppeteer");
const {
  calculateDiscount,
  retryRequestWithProxyRotation,
  retryPuppeteerWithProxyRotation,
} = require("./utils/helper");

axios.defaults.timeout = 180000;

// ============================================================================
// CONFIGURATION
// ============================================================================

const STORE_CONFIG = {
  NEIMAN_MARCUS: {
    name: "Neiman Marcus",
    domain: "neimanmarcus.com",
    currency: "USD",
    country: "US",
    returnPolicy:
      "https://www.neimanmarcus.com/assistance/assistance.jsp#returns",
  },
};

const NEIMAN_MARCUS_CATEGORIES = {
  WOMENS_CLOTHING: {
    name: "Women's Clothing",
    url: "/c/womens-clothing-cat58290731?navpath=cat000000_cat000001",
  },
  MENS_CLOTHING: {
    name: "Men's Clothing",
    url: "/c/mens-clothing-cat14120827?navpath=cat000000_cat82040732",
  },
};

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
// NEIMAN MARCUS CRAWLER FUNCTIONS
// ============================================================================

/**
 * Helper function to aggressively close all modals
 * Closes: International shipping modal and other pop-ups
 */
async function closeAllModals(page) {
  // Close "International Shipping Unavailable" modal (Neiman Marcus)
  try {
    const hasModal = await page.evaluate(() => {
      const modal = document.querySelector('div.nm-modal[aria-modal="true"]');
      if (modal) {
        const buttons = modal.querySelectorAll("button");
        const continueBtn = Array.from(buttons).find((b) =>
          b.textContent.includes("Continue Shopping")
        );
        if (continueBtn) {
          continueBtn.click();
          return true;
        }
      }
      return false;
    });

    if (hasModal) {
      await page
        .waitForSelector('div.nm-modal[aria-modal="true"]', {
          hidden: true,
          timeout: 3000,
        })
        .catch(() => {});
    }
  } catch {}

  // Close any generic close buttons
  try {
    await page.evaluate(() => {
      const closeButtons = document.querySelectorAll(
        'button.close, .close-icon, [aria-label*="Close"]'
      );
      closeButtons.forEach((btn) => btn.click());
    });
  } catch {}
}

/**
 * Fetch product list from category listing pages
 * Returns basic product info (ID, name, URL)
 */
async function fetchNeimanMarcusProductList(categoryUrl, minProducts = 5) {
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

        const fullUrl = `https://www.neimanmarcus.com${categoryUrl}`;
        console.log(`🌐 Navigating to: ${fullUrl}`);
        await page.goto(fullUrl, {
          waitUntil: "domcontentloaded",
          timeout: 60000,
        });

        // Close all modals
        await closeAllModals(page);

        // Wait for product grid to load
        await page.waitForSelector("div.product-list", {
          visible: true,
          timeout: 30000,
        });

        // Get total products count
        let totalProducts = 0;
        try {
          await page.waitForSelector(".product-list__header__items", {
            visible: true,
            timeout: 10000,
          });
          const totalText = await page.$eval(
            ".product-list__header__items",
            (el) => el.textContent
          );
          const match = totalText.match(/(\d{1,3}(,\d{3})*)/);
          if (match) {
            totalProducts = parseInt(match[1].replace(/,/g, ""));
            console.log(`📦 Total products available: ${totalProducts}`);
          }
        } catch {}

        // Fetch products from all pages
        while (
          allProducts.length < Math.min(minProducts, totalProducts || 20000)
        ) {
          console.log(`📄 Page ${currentPage}...`);

          await page.waitForSelector("div.product-thumbnail", {
            visible: true,
            timeout: 30000,
          });

          // Extract product URLs from current page
          const pageProducts = await page.evaluate(() => {
            const products = [];
            const productElements = document.querySelectorAll(
              "div.product-thumbnail"
            );

            productElements.forEach((el) => {
              try {
                const productId = el.getAttribute("id") || "";
                const link = el.querySelector("a.product-thumbnail__link");
                const productUrl = link?.getAttribute("href") || "";

                const designerEl = el.querySelector(".designer");
                const nameEl = el.querySelector(".name");
                const priceEl = el.querySelector(
                  ".price-no-promo, .currentPrice .price"
                );

                const designer = designerEl?.textContent?.trim() || "";
                const productName = nameEl?.textContent?.trim() || "";
                const price = priceEl?.textContent?.trim() || "";

                if (productId && productUrl && productName) {
                  products.push({
                    id: productId.replace("prod", ""),
                    name: `${designer} ${productName}`.trim(),
                    productUrl: productUrl.startsWith("http")
                      ? productUrl
                      : `https://www.neimanmarcus.com${productUrl}`,
                    price: price,
                  });
                }
              } catch (e) {
                console.log("Error extracting product:", e.message);
              }
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

          // Navigate to next page
          try {
            const nextButton = await page.$("a.arrow-button--right");

            if (!nextButton) {
              console.log("📭 No next button found - end of catalog");
              break;
            }

            const nextHref = await nextButton.evaluate((el) =>
              el.getAttribute("href")
            );

            if (!nextHref) {
              console.log("📭 No href on next button - end of catalog");
              break;
            }

            const url = new URL(nextHref, "https://www.neimanmarcus.com");

            console.log(`➡️  Next page: ${currentPage + 1}`);

            await page.goto(url.toString(), {
              waitUntil: "domcontentloaded",
            });

            // Close all modals after pagination
            await closeAllModals(page);

            // Wait for products to load
            await page.waitForSelector("div.product-thumbnail", {
              visible: true,
              timeout: 30000,
            });

            currentPage++;
          } catch (error) {
            console.log(`❌ Pagination error: ${error.message}`);
            console.log("📭 Stopping pagination");
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

/**
 * Extract detailed product information using Puppeteer
 * Navigates to product page and extracts all colors, sizes, images
 */
async function fetchNeimanMarcusProductDetails(productUrl, browser) {
  const page = await browser.newPage();

  try {
    await page.setViewport({ width: 1920, height: 1080 });
    await page.setUserAgent(
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    );

    console.log(`  🔍 Fetching details: ${productUrl}`);
    await page.goto(productUrl, {
      waitUntil: "domcontentloaded",
      timeout: 60000,
    });

    // Close all modals
    await closeAllModals(page);

    // Wait for product details to load
    await page.waitForSelector('[data-test="pdp-title"]', {
      visible: true,
      timeout: 10000,
    });

    // Extract product details
    const productDetails = await page.evaluate(() => {
      const details = {};

      // Product name and designer
      const designerEl = document.querySelector('[data-test="pdp-designer"]');
      const nameEl = document.querySelector('[data-test="pdp-title"]');

      details.designer = designerEl?.textContent?.trim() || "";
      details.name = nameEl?.textContent?.trim() || "";

      // Price
      const priceEl = document.querySelector(
        ".Pricingstyles__RetailPrice-gnVaue, .price-no-promo"
      );
      const salePriceEl = document.querySelector(
        ".Pricingstyles__PromoPrice-iqMgji .price, .currentPrice .price"
      );

      details.price =
        priceEl?.textContent?.trim().replace(/[^0-9.]/g, "") || "0";
      details.salePrice =
        salePriceEl?.textContent?.trim().replace(/[^0-9.]/g, "") || "";

      // Description
      const descEl = document.querySelector(
        ".ProductMobileAccordionstyles__StyledContainer-dNylnL"
      );
      details.description = descEl?.textContent?.trim() || "";

      // Extract alternate images
      const imageElements = document.querySelectorAll(
        ".ProductMedia__ProductMediaContainer-kJCFRj img"
      );
      details.alternateImages = Array.from(imageElements)
        .map((img) => img.getAttribute("src") || img.getAttribute("data-src"))
        .filter((src) => src && src.includes("http") && !src.includes("svg"))
        .map((src) => {
          // Get high-res version
          if (src.includes("neimanmarcus.com")) {
            return src
              .replace(/dpr_[\d.]+/, "dpr_2.0")
              .replace(/w_\d+/, "w_1200");
          }
          return src;
        });

      details.imageUrl = details.alternateImages[0] || "";

      return details;
    });

    // Extract color options
    const colorSwatches = await page.evaluate(() => {
      const swatches = [];
      const colorLabel = document.querySelector(
        ".ColorOptionsstyles__Label-lnfaBH span"
      );
      const currentColor = colorLabel?.textContent?.trim() || "";

      const swatchElements = document.querySelectorAll(
        '[data-test="pdp-color-swatches"] button'
      );

      swatchElements.forEach((btn) => {
        const img = btn.querySelector("img");
        const colorName = img?.getAttribute("alt") || currentColor;
        const colorImage = img?.getAttribute("src") || "";

        swatches.push({
          colorName: colorName,
          colorImage: colorImage,
        });
      });

      // If no swatches found, use current color
      if (swatches.length === 0 && currentColor) {
        swatches.push({
          colorName: currentColor,
          colorImage: "",
        });
      }

      return swatches;
    });

    console.log(`    🎨 Found ${colorSwatches.length} color(s)`);

    // For each color, extract sizes
    const colorVariants = [];

    for (let colorIdx = 0; colorIdx < colorSwatches.length; colorIdx++) {
      const colorSwatch = colorSwatches[colorIdx];
      const colorName = colorSwatch.colorName;

      // Close all modals before color selection
      await closeAllModals(page);

      // Click on the color swatch (if there are multiple colors)
      if (colorSwatches.length > 1) {
        try {
          const swatchButtons = await page.$$(
            '[data-test="pdp-color-swatches"] button'
          );
          if (swatchButtons[colorIdx]) {
            await swatchButtons[colorIdx].click();

            // Close all modals after color click
            await closeAllModals(page);

            // Wait for color selection to update
            await page.waitForTimeout(2000);
          }
        } catch (e) {
          console.log(
            `    ⚠️ Could not click color swatch ${colorIdx}: ${e.message}`
          );
          await closeAllModals(page);
        }
      }

      // Capture color-specific URL and images
      let colorSpecificData = { colorUrl: "", colorImages: [] };
      try {
        colorSpecificData = await page.evaluate(() => {
          const data = {};

          data.colorUrl = window.location.href;

          // Extract color-specific images
          const imageElements = document.querySelectorAll(
            ".ProductMedia__ProductMediaContainer-kJCFRj img"
          );
          data.colorImages = Array.from(imageElements)
            .map(
              (img) => img.getAttribute("src") || img.getAttribute("data-src")
            )
            .filter(
              (src) => src && src.includes("http") && !src.includes("svg")
            )
            .map((src) => {
              if (src.includes("neimanmarcus.com")) {
                return src
                  .replace(/dpr_[\d.]+/, "dpr_2.0")
                  .replace(/w_\d+/, "w_1200");
              }
              return src;
            });

          return data;
        });

        console.log(`    🔗 Color URL: ${colorSpecificData.colorUrl}`);
        console.log(
          `    🖼️ Found ${colorSpecificData.colorImages.length} images for ${colorName}`
        );
      } catch (e) {
        console.log(
          `    ⚠️ Could not capture color-specific data: ${e.message}`
        );
        colorSpecificData = { colorUrl: "", colorImages: [] };
      }

      // Close all modals after capturing color data
      await closeAllModals(page);

      // Extract sizes
      const allSizes = [];
      try {
        // Try to open size dropdown
        try {
          const sizeSelector = await page.$('[data-test="pdp-size-selector"]');
          if (sizeSelector) {
            await sizeSelector.click();
            await page.waitForTimeout(1000);
          }
        } catch {}

        // Extract sizes from dropdown
        const sizes = await page.evaluate(() => {
          const sizeOptions = document.querySelectorAll(
            'ul[role="listbox"] li[role="option"], .MuiMenuItem-root'
          );

          return Array.from(sizeOptions)
            .map((li) => {
              const sizeText = li.textContent?.trim() || "";
              const isDisabled =
                li.getAttribute("aria-disabled") === "true" ||
                li.classList.contains("Mui-disabled");

              if (sizeText && sizeText !== "Size") {
                return {
                  size: sizeText,
                  available: !isDisabled,
                };
              }
              return null;
            })
            .filter((s) => s !== null);
        });

        allSizes.push(...sizes);

        // Close dropdown
        try {
          await page.keyboard.press("Escape");
        } catch {}
      } catch (e) {
        console.log(`    ⚠️ Error extracting sizes: ${e.message}`);
        await closeAllModals(page);
      }

      console.log(`    📏 Total ${allSizes.length} size(s) for ${colorName}`);

      // Only save if we have real data
      if (allSizes.length === 0) {
        console.log(
          `    ⚠️ No sizes found for ${colorName}, skipping this color`
        );
        continue;
      }

      if (
        !colorSpecificData.colorUrl ||
        colorSpecificData.colorImages.length === 0
      ) {
        console.log(
          `    ⚠️ Missing color-specific data for ${colorName}, skipping this color`
        );
        continue;
      }

      colorVariants.push({
        color: colorName,
        sizes: allSizes,
        colorUrl: colorSpecificData.colorUrl,
        colorImages: colorSpecificData.colorImages,
      });
    }

    await page.close();

    return {
      ...productDetails,
      colorVariants: colorVariants,
    };
  } catch (error) {
    console.log(`    ❌ Error fetching product details: ${error.message}`);
    await page.close();
    return null;
  }
}

// ============================================================================
// MAIN CRAWLER FUNCTION
// ============================================================================

async function neimanMarcusMain(minProductsPerCategory = 1600) {
  const store = STORE_CONFIG.NEIMAN_MARCUS;
  const inc = startIncrementalCatalog(store.country, "neimanmarcus", store);

  let totalProducts = 0;
  let categoryCount = 0;
  const totalCategories = Object.keys(NEIMAN_MARCUS_CATEGORIES).length;

  const processedProductIds = new Set();

  try {
    for (const [key, category] of Object.entries(NEIMAN_MARCUS_CATEGORIES)) {
      categoryCount++;
      console.log(
        `\n🏪 Category ${categoryCount}/${totalCategories}: ${category.name}`
      );

      // Step 1: Fetch all product URLs from category listing
      const productList = await fetchNeimanMarcusProductList(
        category.url,
        minProductsPerCategory
      );

      console.log(`\n📦 Collected ${productList.length} product URLs`);
      console.log(`🔍 Now fetching detailed information for each product...\n`);

      // Step 2: Collect all products in memory
      const categoryProducts = [];

      // Open a browser for detail extraction
      const result = await retryPuppeteerWithProxyRotation(
        async (browser) => {
          for (let i = 0; i < productList.length; i++) {
            const basicProduct = productList[i];

            // Check if product already processed (deduplication)
            if (processedProductIds.has(basicProduct.id)) {
              console.log(
                `\n[${i + 1}/${productList.length}] ⏭️ Skipping duplicate: ${
                  basicProduct.name
                } (${basicProduct.id})`
              );
              continue;
            }

            console.log(
              `\n[${i + 1}/${productList.length}] Processing: ${
                basicProduct.name
              } (${basicProduct.id})`
            );

            try {
              // Fetch detailed product info
              const detail = await fetchNeimanMarcusProductDetails(
                basicProduct.productUrl,
                browser
              );

              if (
                !detail ||
                !detail.colorVariants ||
                detail.colorVariants.length === 0
              ) {
                console.log(`  ⚠️ Skipping - no detail or variants found`);
                continue;
              }

              const designer = detail.designer || "";
              const name = detail.name || basicProduct.name;
              const description = (detail.description || "")
                .replace(/<[^>]*>/g, " ")
                .replace(/\s+/g, " ")
                .trim();
              const images = detail.alternateImages || [];
              const imageUrl = detail.imageUrl || images[0] || "";
              const brand = designer || "Neiman Marcus";
              const domain = "neimanmarcus.com";
              const parentId = basicProduct.id;
              const price = detail.price || basicProduct.price || "0";
              const salePrice = detail.salePrice || "";
              const originalPriceNum = parseFloat(price) || 0;
              const salePriceNum = salePrice ? parseFloat(salePrice) : 0;
              const finalPrice =
                salePriceNum > 0 ? salePriceNum : originalPriceNum;
              const isOnSale =
                salePriceNum > 0 && salePriceNum < originalPriceNum;

              // Determine category name from URL
              let categoryName = "Clothing";
              if (basicProduct.productUrl.includes("womens")) {
                categoryName = "Women's Clothing";
              } else if (basicProduct.productUrl.includes("mens")) {
                categoryName = "Men's Clothing";
              }

              // Determine gender
              let gender = "";
              if (basicProduct.productUrl.includes("womens")) gender = "Female";
              else if (basicProduct.productUrl.includes("mens"))
                gender = "Male";

              // Build variants matrix (color x size)
              const variants = [];
              for (const colorVariant of detail.colorVariants) {
                const color = colorVariant.color;
                const sizes = colorVariant.sizes || [];
                const variantUrl = colorVariant.colorUrl;
                const variantImages = colorVariant.colorImages;
                const variantImageUrl = variantImages[0];

                if (sizes.length === 0) {
                  console.log(`  ⚠️ No sizes for color ${color}, skipping`);
                  continue;
                }

                if (
                  !variantUrl ||
                  !variantImages ||
                  variantImages.length === 0
                ) {
                  console.log(`  ⚠️ Missing data for color ${color}, skipping`);
                  continue;
                }

                for (const sizeInfo of sizes) {
                  const size = sizeInfo.size;
                  const isInStock = sizeInfo.available;

                  variants.push({
                    price_currency: "USD",
                    original_price: originalPriceNum,
                    link_url: variantUrl,
                    deeplink_url: variantUrl,
                    image_url: variantImageUrl,
                    alternate_image_urls: variantImages,
                    is_on_sale: isOnSale,
                    is_in_stock: isInStock,
                    size: size,
                    color: color,
                    mpn: uuidv5(
                      `${parentId}-${color}`,
                      "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
                    ),
                    ratings_count: 0,
                    average_ratings: 0,
                    review_count: 0,
                    selling_price: originalPriceNum,
                    sale_price: salePriceNum,
                    final_price: finalPrice,
                    discount: isOnSale
                      ? Math.round(
                          ((originalPriceNum - salePriceNum) /
                            originalPriceNum) *
                            100
                        )
                      : 0,
                    operation_type: "INSERT",
                    variant_id: uuidv5(
                      `${parentId}-${color}-${size}`,
                      "6ba7b810-9dad-11d1-80b4-00c04fd430c1"
                    ),
                    variant_description: "",
                  });
                }
              }

              if (variants.length === 0) {
                console.log(`  ⚠️ Skipping - no variants generated`);
                continue;
              }

              const formattedProduct = {
                parent_product_id: parentId,
                name: name,
                description: description,
                category: categoryName,
                retailer_domain: domain,
                brand: brand,
                gender: gender,
                materials: null,
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

              categoryProducts.push(formattedProduct);
              processedProductIds.add(parentId);

              console.log(
                `  ✅ Added product with ${variants.length} variant(s)`
              );
            } catch (error) {
              console.log(`  ❌ Error processing product: ${error.message}`);
            }
          }

          return categoryProducts;
        },
        3,
        2000,
        "US"
      );

      // Step 3: Write all products at once
      console.log(
        `\n📝 Writing ${categoryProducts.length} products for ${category.name}...`
      );
      for (const product of categoryProducts) {
        try {
          appendProductIncremental(inc, product);
          totalProducts++;
        } catch (e) {
          console.log(`Failed writing product: ${e.message}`);
        }
      }

      console.log(
        `✅ Completed ${category.name}: ${categoryProducts.length} products written`
      );

      if (categoryCount < totalCategories) {
        console.log(
          `📁 Progress: ${categoryCount}/${totalCategories} categories completed, ${totalProducts} total products written`
        );
      }
    }
  } catch (error) {
    console.error(`\n❌ Error during crawling: ${error.message}`);
    console.error(error.stack);
  } finally {
    try {
      const files = finalizeIncrementalCatalog(inc);
      console.log(
        `\n📦 Catalog finalized. Total unique products written: ${totalProducts}`
      );
      console.log(
        `🔍 Total unique product IDs processed: ${processedProductIds.size}`
      );
      return {
        jsonPath: files.jsonPath,
        totalProductIds: totalProducts,
      };
    } catch (finalizeError) {
      console.error(`\n❌ Error finalizing catalog: ${finalizeError.message}`);
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

async function runNeimanMarcusCrawler(options = {}) {
  const { minProductsPerCategory = 1600 } = options;

  console.log("🏪 Starting Neiman Marcus Crawler...");
  console.log(`🎯 Target: ${minProductsPerCategory} products per category`);

  try {
    const result = await neimanMarcusMain(minProductsPerCategory);

    if (result) {
      console.log("\n✅ Neiman Marcus crawling completed successfully!");
      console.log(`📁 Files generated: ${result.jsonPath}`);
      console.log(`📊 Total products processed: ${result.totalProductIds}`);
      return result;
    } else {
      console.log("\n❌ Neiman Marcus crawling failed");
      return false;
    }
  } catch (error) {
    console.error("\n💥 Neiman Marcus crawler error:", error.message);
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

  runNeimanMarcusCrawler(options)
    .then((result) => {
      if (result) {
        console.log("\n🎉 Neiman Marcus crawler finished successfully!");
        process.exit(0);
      } else {
        console.log("\n❌ Neiman Marcus crawler failed");
        process.exit(1);
      }
    })
    .catch((error) => {
      console.error("💥 Unexpected error:", error);
      process.exit(1);
    });
}

module.exports = {
  runNeimanMarcusCrawler,
  neimanMarcusMain,
};
