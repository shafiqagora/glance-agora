/** @format */
/**
 * Madewell Crawler - Single File Script
 * Complete crawler for Madewell products with streaming to avoid memory issues
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
  MADEWELL: {
    name: "Madewell",
    domain: "madewell.com",
    currency: "USD",
    country: "US",
    returnPolicy: "https://www.madewell.com/us/c/returns/",
  },
};

const MADEWELL_CATEGORIES = {
  WOMENS_CLOTHING: {
    name: "Women's Clothing",
    url: "/pk/womens/clothing/",
  },
  MENS_CLOTHING: {
    name: "Men's Clothing",
    url: "/pk/mens/clothing/",
  },
};

// ============================================================================
// HELPER FUNCTIONS (Using shared utilities from helper.js)
// ============================================================================

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
// MADEWELL CRAWLER FUNCTIONS
// ============================================================================

/**
 * Extract detailed product information using Puppeteer
 * Navigates to product page and extracts all colors, sizes, images
 */
async function fetchMadewellProductDetailsPuppeteer(productUrl, browser) {
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

    // Close modal if present
    try {
      const modalClose = await page.$('[data-testid="modal-close"]');
      if (modalClose) {
        await modalClose.click();
        await page
          .waitForSelector('[data-testid="modal-close"]', {
            hidden: true,
          })
          .catch(() => {});
      }
    } catch {}

    // Wait for product details to load
    await page.waitForSelector(
      "h1.ProductTopDetailsReimagined_pdpName__8wtYV",
      {
        visible: true,
        timeout: 10000,
      }
    );

    // Extract product details
    const productDetails = await page.evaluate(() => {
      const details = {};

      // Product name
      const nameEl = document.querySelector(
        "h1.ProductTopDetailsReimagined_pdpName__8wtYV"
      );
      details.name = nameEl?.textContent?.trim() || "";

      // Price
      const priceEl = document.querySelector(
        'span[aria-label="Final Sale Price"][role="productSalePrice"], .PriceAndPromotionsReimagined_pdpPrices__7FcAn span'
      );
      const priceText = priceEl?.textContent?.trim() || "";
      details.price = priceText.replace(/[^0-9.]/g, "");

      // Description
      const descEl = document.querySelector('p[data-testid="description"]');
      details.description = descEl?.textContent?.trim() || "";

      // Materials from description list
      const detailsList = document.querySelectorAll(
        'ul[data-testid="shotDescription"] li'
      );
      const materialsText = Array.from(detailsList)
        .map((li) => li.textContent?.trim())
        .find(
          (text) =>
            text &&
            (text.includes("%") ||
              text.toLowerCase().includes("cotton") ||
              text.toLowerCase().includes("polyester"))
        );
      details.materials = materialsText || "";

      // Extract alternate images
      const imageButtons = document.querySelectorAll(
        "button.ImagesReimagined_pdpGridImage__iddW_ img[data-test-id='productGallery']"
      );
      details.alternateImages = Array.from(imageButtons)
        .map((img) => img.getAttribute("src"))
        .filter((src) => src && src.includes("http"))
        .map((src) => {
          // Get the high-res version
          const url = new URL(src);
          url.searchParams.set("wid", "1400");
          url.searchParams.set("hei", "1779");
          return url.toString();
        });

      details.imageUrl = details.alternateImages[0] || "";

      return details;
    });

    // Extract all color options
    const colorSwatches = await page.evaluate(() => {
      const swatches = document.querySelectorAll(
        'button[data-role="swatch"] img[alt]'
      );
      return Array.from(swatches).map((img) => ({
        colorName: img.getAttribute("alt") || "",
        colorImage: img.getAttribute("src") || "",
      }));
    });

    console.log(`    🎨 Found ${colorSwatches.length} color(s)`);

    // For each color, extract sizes (including all fit types)
    const colorVariants = [];

    for (let colorIdx = 0; colorIdx < colorSwatches.length; colorIdx++) {
      const colorSwatch = colorSwatches[colorIdx];
      const colorName = colorSwatch.colorName;

      // Close any modal that might be blocking
      try {
        const modal = await page.$('[data-testid="modal-close"]');
        if (modal) {
          await modal.click();
          await page
            .waitForSelector('[data-testid="modal-close"]', { hidden: true })
            .catch(() => {});
        }
      } catch {}

      // Click on the color swatch
      try {
        const swatchButtons = await page.$$('button[data-role="swatch"]');
        if (swatchButtons[colorIdx]) {
          await swatchButtons[colorIdx].click();
          // Wait for color selection to update
          await page
            .waitForSelector(
              'button[data-role="swatch"].SwatchItem_swatchSelected__UilzE',
              {
                visible: true,
                timeout: 3000,
              }
            )
            .catch(() => {});
        }
      } catch (e) {
        console.log(`    ⚠️ Could not click color swatch ${colorIdx}`);
        continue;
      }

      // Check if there are fit type buttons (Standard, Petite, Tall)
      const fitTypes = await page.evaluate(() => {
        const fitButtons = document.querySelectorAll(
          'button[data-testid="extenedSize"]'
        );
        return Array.from(fitButtons).map((btn) => {
          const fitName = btn.querySelector("span")?.textContent?.trim() || "";
          const fitId = btn.getAttribute("id") || "";
          return { fitName, fitId };
        });
      });

      const allSizes = [];

      if (fitTypes.length > 0) {
        // Product has multiple fit types - click through each
        console.log(
          `    👔 Found ${fitTypes.length} fit type(s): ${fitTypes
            .map((f) => f.fitName)
            .join(", ")}`
        );

        for (const fitType of fitTypes) {
          try {
            // Click the fit type button
            await page.click(`button[id="${fitType.fitId}"]`);
            await page
              .waitForSelector(
                "ul.SizeAttributeOptions_pdpVariantListDropdownList__UauEm",
                {
                  visible: true,
                  timeout: 3000,
                }
              )
              .catch(() => {});

            // Try to click size dropdown to reveal sizes
            try {
              const dropdown = await page.$(
                "button.VariationAttributeReimagined_pdpVariantListDropdownHead__csH9l"
              );
              if (dropdown) {
                const isOpen = await page.evaluate(() => {
                  const dd = document.querySelector(
                    "button.VariationAttributeReimagined_pdpVariantListDropdownHead__csH9l"
                  );
                  return dd?.getAttribute("aria-expanded") === "true";
                });

                if (!isOpen) {
                  await page.click(
                    "button.VariationAttributeReimagined_pdpVariantListDropdownHead__csH9l"
                  );
                  await page.waitForSelector(
                    "ul.SizeAttributeOptions_pdpVariantListDropdownList__UauEm",
                    {
                      visible: true,
                      timeout: 5000,
                    }
                  );
                }
              }
            } catch (e) {
              console.log(
                `    ⚠️ Could not open size dropdown for fit: ${fitType.fitName}`
              );
              continue;
            }

            // Extract sizes for this fit type
            const fitSizes = await page.evaluate((fit) => {
              const sizeItems = document.querySelectorAll(
                'ul.SizeAttributeOptions_pdpVariantListDropdownList__UauEm li[role="option"]'
              );
              return Array.from(sizeItems)
                .map((li) => {
                  const sizeText =
                    li
                      .querySelector(
                        "span.SizeAttributeOptions_dropdownListItemValue__39dxc"
                      )
                      ?.textContent?.trim() || "";
                  const isOutOfStock = li.classList.contains(
                    "SizeAttributeOptions_dropdownListItemOos__FtGsr"
                  );
                  const isDisabled =
                    li.getAttribute("aria-disabled") === "true";
                  return {
                    size: `${sizeText} (${fit})`,
                    available: !isOutOfStock && !isDisabled,
                  };
                })
                .filter((s) => s.size && s.size.length > 0);
            }, fitType.fitName);

            console.log(
              `      📏 ${fitSizes.length} size(s) for ${fitType.fitName}`
            );
            allSizes.push(...fitSizes);

            // Close dropdown
            try {
              const isOpen = await page.evaluate(() => {
                const dd = document.querySelector(
                  "button.VariationAttributeReimagined_pdpVariantListDropdownHead__csH9l"
                );
                return dd?.getAttribute("aria-expanded") === "true";
              });
              if (isOpen) {
                await page.click(
                  "button.VariationAttributeReimagined_pdpVariantListDropdownHead__csH9l"
                );
              }
            } catch {}
          } catch (e) {
            console.log(
              `    ⚠️ Error processing fit type ${fitType.fitName}: ${e.message}`
            );
          }
        }
      } else {
        // No fit types - standard size extraction
        try {
          const dropdown = await page.$(
            "button.VariationAttributeReimagined_pdpVariantListDropdownHead__csH9l"
          );
          if (dropdown) {
            const isOpen = await page.evaluate(() => {
              const dd = document.querySelector(
                "button.VariationAttributeReimagined_pdpVariantListDropdownHead__csH9l"
              );
              return dd?.getAttribute("aria-expanded") === "true";
            });

            if (!isOpen) {
              await page.click(
                "button.VariationAttributeReimagined_pdpVariantListDropdownHead__csH9l"
              );
              await page.waitForSelector(
                "ul.SizeAttributeOptions_pdpVariantListDropdownList__UauEm",
                {
                  visible: true,
                  timeout: 5000,
                }
              );
            }
          }

          // Extract sizes
          const sizes = await page.evaluate(() => {
            const sizeItems = document.querySelectorAll(
              'ul.SizeAttributeOptions_pdpVariantListDropdownList__UauEm li[role="option"]'
            );
            return Array.from(sizeItems)
              .map((li) => {
                const sizeText =
                  li
                    .querySelector(
                      "span.SizeAttributeOptions_dropdownListItemValue__39dxc"
                    )
                    ?.textContent?.trim() || "";
                const isOutOfStock = li.classList.contains(
                  "SizeAttributeOptions_dropdownListItemOos__FtGsr"
                );
                const isDisabled = li.getAttribute("aria-disabled") === "true";
                return {
                  size: sizeText,
                  available: !isOutOfStock && !isDisabled,
                };
              })
              .filter((s) => s.size && s.size.length > 0);
          });

          allSizes.push(...sizes);

          // Close dropdown
          try {
            const isOpen = await page.evaluate(() => {
              const dd = document.querySelector(
                "button.VariationAttributeReimagined_pdpVariantListDropdownHead__csH9l"
              );
              return dd?.getAttribute("aria-expanded") === "true";
            });
            if (isOpen) {
              await page.click(
                "button.VariationAttributeReimagined_pdpVariantListDropdownHead__csH9l"
              );
            }
          } catch {}
        } catch (e) {
          console.log(
            `    ⚠️ Could not open size dropdown for color: ${colorName}`
          );
          continue;
        }
      }

      console.log(`    📏 Total ${allSizes.length} size(s) for ${colorName}`);

      colorVariants.push({
        color: colorName,
        sizes: allSizes,
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

/**
 * Fetch product list from category listing pages
 * Returns basic product info (ID, name, URL)
 */
async function fetchMadewellProductList(categoryUrl, minProducts = 400) {
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

        const fullUrl = `https://www.madewell.com${categoryUrl}?country=US&currency=USD`;
        console.log(`🌐 Navigating to: ${fullUrl}`);
        await page.goto(fullUrl, {
          waitUntil: "domcontentloaded",
          timeout: 60000,
        });

        // Set locale
        await page.evaluate(() => {
          document.cookie = "country=US; path=/";
          document.cookie = "currency=USD; path=/";
          document.cookie = "locale=en-US; path=/";
        });

        // Wait for product grid to load
        await page.waitForSelector("ul.ProductsGrid_plpGrid__OP3wT", {
          visible: true,
        });

        // Close popup modals if present
        try {
          const closeButton = await page.$('[data-testid="modal-close"]');
          if (closeButton) {
            await closeButton.click();
          }
        } catch {}

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

          // Close modal before pagination
          try {
            const modalCloseButton = await page.$(
              '[data-testid="modal-close"]'
            );
            if (modalCloseButton) {
              await modalCloseButton.click();
              await page
                .waitForSelector('[data-testid="modal-close"]', {
                  hidden: true,
                })
                .catch(() => {});
            }
          } catch {}

          // Navigate to next page
          try {
            const nextButton = await page.$(
              "a.Pagination_plpPaginationNext__2S2dh"
            );

            if (!nextButton) {
              console.log("📭 No more pages");
              break;
            }

            const nextHref = await nextButton.evaluate((el) =>
              el.getAttribute("href")
            );

            if (!nextHref) break;

            const url = new URL(nextHref, "https://www.madewell.com");
            url.searchParams.set("country", "US");
            url.searchParams.set("currency", "USD");

            console.log(`➡️  Next page: ${currentPage + 1}`);
            await Promise.all([
              page.waitForNavigation({
                waitUntil: "domcontentloaded",
                timeout: 60000,
              }),
              page.goto(url.toString(), { timeout: 60000 }),
            ]);

            await page.waitForSelector(
              "li.ProductsGrid_plpGridElement__aSWFa",
              {
                visible: true,
              }
            );

            // Close modal after page load
            try {
              const modalAfterNav = await page.$('[data-testid="modal-close"]');
              if (modalAfterNav) await modalAfterNav.click();
            } catch {}

            currentPage++;
          } catch (error) {
            console.log(`❌ Pagination error: ${error.message}`);
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

async function madewellMain(minProductsPerCategory = 400) {
  const store = STORE_CONFIG.MADEWELL;
  const inc = startIncrementalCatalog(store.country, "madewell", store);

  let totalProducts = 0;
  let categoryCount = 0;
  const totalCategories = Object.keys(MADEWELL_CATEGORIES).length;

  for (const [key, category] of Object.entries(MADEWELL_CATEGORIES)) {
    categoryCount++;
    console.log(
      `\n🏪 Category ${categoryCount}/${totalCategories}: ${category.name}`
    );

    // Step 1: Fetch all product URLs from category listing
    const productList = await fetchMadewellProductList(
      category.url,
      minProductsPerCategory
    );

    console.log(`\n📦 Collected ${productList.length} product URLs`);
    console.log(`🔍 Now fetching detailed information for each product...\n`);

    // Step 2: Collect all products in memory (not streaming)
    const categoryProducts = [];

    // Open a browser for detail extraction
    const result = await retryPuppeteerWithProxyRotation(
      async (browser) => {
        for (let i = 0; i < productList.length; i++) {
          const basicProduct = productList[i];
          console.log(
            `\n[${i + 1}/${productList.length}] Processing: ${
              basicProduct.name
            } (${basicProduct.id})`
          );

          try {
            // Fetch detailed product info
            const detail = await fetchMadewellProductDetailsPuppeteer(
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

            const name = detail.name || basicProduct.name;
            const description = (detail.description || "")
              .replace(/<[^>]*>/g, " ")
              .replace(/\s+/g, " ")
              .trim();
            const images = detail.alternateImages || [];
            const imageUrl = detail.imageUrl || images[0] || "";
            const brand = "Madewell";
            const domain = "madewell.com";
            const parentId = basicProduct.id;
            const price = detail.price || 0;
            const originalPriceNum = parseFloat(price) || 0;
            const materials = detail.materials || null;

            // Determine category name
            let categoryName = category.name;
            const urlParts = basicProduct.productUrl.split("/");
            const clothingIdx = urlParts.findIndex((p) => p === "clothing");
            if (clothingIdx !== -1 && clothingIdx < urlParts.length - 1) {
              categoryName = urlParts
                .slice(clothingIdx, clothingIdx + 2)
                .join(" > ");
            }

            // Determine gender
            let gender = "";
            const urlLower = basicProduct.productUrl.toLowerCase();
            if (/\/womens\//.test(urlLower)) gender = "Female";
            else if (/\/mens\//.test(urlLower)) gender = "Male";

            // Build variants matrix (color x size)
            const variants = [];
            for (const colorVariant of detail.colorVariants) {
              const color = colorVariant.color;
              const sizes = colorVariant.sizes || [];

              if (sizes.length === 0) {
                // No sizes, create single variant
                sizes.push({ size: "One Size", available: true });
              }

              for (const sizeInfo of sizes) {
                const size = sizeInfo.size;
                const isInStock = sizeInfo.available;

                variants.push({
                  price_currency: "USD",
                  original_price: originalPriceNum,
                  link_url: basicProduct.productUrl,
                  deeplink_url: basicProduct.productUrl,
                  image_url: imageUrl,
                  alternate_image_urls: images,
                  is_on_sale: false,
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
                  sale_price: null,
                  final_price: originalPriceNum,
                  discount: 0,
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
              materials: materials,
              return_policy_link: "https://www.madewell.com/us/c/returns/",
              return_policy:
                "Madewell gladly accepts returns of unworn, unwashed, undamaged or defective merchandise for full refund or exchange within 30 days of the original purchase.",
              size_chart: null,
              available_bank_offers: "",
              available_coupons: "",
              variants: variants,
              operation_type: "INSERT",
              source: "madewell",
            };

            categoryProducts.push(formattedProduct);
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

  const files = finalizeIncrementalCatalog(inc);
  console.log(
    `\n📦 Final completion. Total products written: ${totalProducts}`
  );
  return {
    jsonPath: files.jsonPath,
    totalProductIds: totalProducts,
  };
}

// ============================================================================
// MAIN ENTRY POINT
// ============================================================================

async function runMadewellCrawler(options = {}) {
  const { minProductsPerCategory = 400 } = options;

  console.log("🏪 Starting Madewell Crawler...");
  console.log(`🎯 Target: ${minProductsPerCategory} products per category`);

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

  // Parse command line arguments
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
