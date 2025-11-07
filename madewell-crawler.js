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
 * Helper function to aggressively close all modals (BACKUP)
 * Closes: Ship To modal, Sign Up modal, and generic modals
 * Note: Cookies are set to prevent modals, but this is a backup
 */
async function closeAllModals(page) {
  // Close "Ship To" country modal (if it still appears)
  try {
    const shipToModal = await page.$(
      "button.ShipToModal_continueShoppingBtn__QubmY"
    );
    if (shipToModal) {
      console.log("  ⚠️ Ship To modal appeared despite cookies, closing...");
      await shipToModal.click();
      // ⭐ Wait for modal to disappear instead of timeout
      await page
        .waitForSelector("button.ShipToModal_continueShoppingBtn__QubmY", {
          hidden: true,
          timeout: 3000,
        })
        .catch(() => {});
    }
  } catch {}

  // Close "Sign Up / Create Account" modal
  try {
    const signUpModal = await page.$('[data-testid="domestic-auth-modal"]');
    if (signUpModal) {
      const closeBtn = await signUpModal.$('button[aria-label="Close modal"]');
      if (closeBtn) {
        await closeBtn.click();
        // ⭐ Wait for modal to disappear instead of timeout
        await page
          .waitForSelector('[data-testid="domestic-auth-modal"]', {
            hidden: true,
            timeout: 3000,
          })
          .catch(() => {});
      }
    }
  } catch {}

  // Close generic modals with [data-testid="modal-close"]
  try {
    const modalClose = await page.$('[data-testid="modal-close"]');
    if (modalClose) {
      await modalClose.click();
      // ⭐ Wait for modal to disappear instead of timeout
      await page
        .waitForSelector('[data-testid="modal-close"]', {
          hidden: true,
          timeout: 3000,
        })
        .catch(() => {});
    }
  } catch {}
}

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

    // ⭐ Set cookies BEFORE navigation to prevent Ship To modal
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

    console.log(`  🔍 Fetching details: ${productUrl}`);
    await page.goto(productUrl, {
      waitUntil: "domcontentloaded",
      timeout: 60000,
    });

    // Close all modals (Ship To, Sign Up, etc.) - as backup
    await closeAllModals(page);

    // Wait for product details to load
    await page.waitForSelector(
      "h1.ProductTopDetailsReimagined_pdpName__8wtYV",
      {
        visible: true,
        timeout: 30000, // Increased to 30 seconds
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

      // Close all modals before color selection
      await closeAllModals(page);

      // Click on the color swatch
      try {
        const swatchButtons = await page.$$('button[data-role="swatch"]');
        if (swatchButtons[colorIdx]) {
          await swatchButtons[colorIdx].click();

          // Close all modals after color click
          await closeAllModals(page);

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

          // ⭐ CRITICAL: Wait for size dropdown to be ready after color change (NO TIMEOUT)
          await page
            .waitForSelector(
              "button.VariationAttributeReimagined_pdpVariantListDropdownHead__csH9l",
              {
                visible: true,
                timeout: 5000,
              }
            )
            .catch(() => {});

          // ⭐ Wait for image gallery to update (check images are loaded)
          await page
            .waitForFunction(
              () => {
                const images = document.querySelectorAll(
                  "button.ImagesReimagined_pdpGridImage__iddW_ img[data-test-id='productGallery']"
                );
                return images.length > 0;
              },
              { timeout: 5000 }
            )
            .catch(() => {});
        }
      } catch (e) {
        console.log(
          `    ⚠️ Could not click color swatch ${colorIdx}: ${e.message}`
        );
        // Close all modals and try to continue anyway
        await closeAllModals(page);
        // Don't skip - try to extract data with current state
      }

      // ⭐ CAPTURE COLOR-SPECIFIC URL AND IMAGES AFTER CLICKING COLOR
      let colorSpecificData = { colorUrl: "", colorImages: [] };
      try {
        colorSpecificData = await page.evaluate(() => {
          const data = {};

          // Get current URL with ccode parameter
          // Replace /PK/ or /pk/ with /us/ for United States (case-insensitive)
          data.colorUrl = window.location.href.replace(/\/PK\//i, "/us/");

          // Extract color-specific images
          const imageButtons = document.querySelectorAll(
            "button.ImagesReimagined_pdpGridImage__iddW_ img[data-test-id='productGallery']"
          );
          data.colorImages = Array.from(imageButtons)
            .map((img) => img.getAttribute("src"))
            .filter((src) => src && src.includes("http"))
            .map((src) => {
              // Get the high-res version
              const url = new URL(src);
              url.searchParams.set("wid", "1400");
              url.searchParams.set("hei", "1779");
              return url.toString();
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
        // Will be skipped later if empty - NO FALLBACK
        colorSpecificData = { colorUrl: "", colorImages: [] };
      }

      // ⭐ Close all modals after capturing color data, before accessing size dropdown
      await closeAllModals(page);
      // Modals now wait for disappearance inside closeAllModals() - NO TIMEOUT NEEDED

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
            // Close all modals before clicking fit type
            await closeAllModals(page);

            // Click the fit type button
            await page.click(`button[id="${fitType.fitId}"]`);

            // Close all modals after clicking fit type
            await closeAllModals(page);

            await page
              .waitForSelector(
                "ul.SizeAttributeOptions_pdpVariantListDropdownList__UauEm",
                {
                  visible: true,
                  timeout: 3000,
                }
              )
              .catch(() => {});

            // Try to click size dropdown to reveal sizes (with retry)
            let dropdownOpened = false;
            for (let retry = 0; retry < 3 && !dropdownOpened; retry++) {
              try {
                // Close all modals before attempting to open dropdown
                await closeAllModals(page);
                // Modals now wait for disappearance - NO TIMEOUT NEEDED

                // Check if sizes are already visible (dropdown already open)
                const sizesAlreadyVisible = await page.$(
                  "ul.SizeAttributeOptions_pdpVariantListDropdownList__UauEm"
                );

                if (sizesAlreadyVisible) {
                  // Sizes are already visible, no need to open dropdown
                  dropdownOpened = true;
                  break;
                }

                // Try to find and click dropdown button
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
                  dropdownOpened = true;
                }
              } catch (e) {
                if (retry === 2) {
                  console.log(
                    `    ⚠️ Could not open size dropdown for fit: ${fitType.fitName} after 3 attempts`
                  );
                  // Close all modals before continuing
                  await closeAllModals(page);
                }
              }
            }

            if (!dropdownOpened) {
              // Close all modals and try to extract anyway
              await closeAllModals(page);
              console.log(
                `    ℹ️ Attempting to extract sizes for ${fitType.fitName} without dropdown...`
              );
              // Don't skip - try to extract visible sizes
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
            // Close all modals after error before moving to next fit type
            await closeAllModals(page);
          }
        }
      } else {
        // No fit types - standard size extraction (with retry)
        let dropdownOpened = false;
        for (let retry = 0; retry < 3 && !dropdownOpened; retry++) {
          try {
            // Close all modals before attempting to open dropdown
            await closeAllModals(page);
            // Modals now wait for disappearance - NO TIMEOUT NEEDED

            // Check if sizes are already visible (dropdown already open)
            const sizesAlreadyVisible = await page.$(
              "ul.SizeAttributeOptions_pdpVariantListDropdownList__UauEm"
            );

            if (sizesAlreadyVisible) {
              // Sizes are already visible, no need to open dropdown
              dropdownOpened = true;
              break;
            }

            // Try to find and click dropdown button
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
              dropdownOpened = true;
            }
          } catch (e) {
            if (retry === 2) {
              console.log(
                `    ⚠️ Could not open size dropdown for color: ${colorName} after 3 attempts`
              );
              // Close all modals before continuing
              await closeAllModals(page);
            }
          }
        }

        if (!dropdownOpened) {
          // Close all modals and try to extract anyway
          await closeAllModals(page);
          console.log(
            `    ℹ️ Attempting to extract sizes for ${colorName} without dropdown...`
          );
          // Don't skip - try to extract visible sizes
        }

        try {
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
          console.log(`    ⚠️ Error extracting sizes: ${e.message}`);
          // Close all modals after error
          await closeAllModals(page);
        }
      }

      console.log(`    📏 Total ${allSizes.length} size(s) for ${colorName}`);

      // ⭐ Only save if we have real sizes - no fallback
      if (allSizes.length === 0) {
        console.log(
          `    ⚠️ No sizes found for ${colorName}, skipping this color`
        );
        continue; // Skip this color if no real sizes
      }

      // ⭐ Only save if we have real color data
      if (
        !colorSpecificData.colorUrl ||
        colorSpecificData.colorImages.length === 0
      ) {
        console.log(
          `    ⚠️ Missing color-specific data for ${colorName}, skipping this color`
        );
        continue; // Skip if missing real data
      }

      colorVariants.push({
        color: colorName,
        sizes: allSizes, // Only real sizes
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

/**
 * Fetch product list from category listing pages
 * Returns basic product info (ID, name, URL)
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

        // ⭐ Set cookies BEFORE navigation to prevent Ship To modal
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
          timeout: 120000, // 2 minutes for page load
        });

        // Close all modals (Ship To, Sign Up, etc.)
        await closeAllModals(page);

        // Wait for product grid to load
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

  // ⭐ Track unique product IDs across all categories to prevent duplicates
  const processedProductIds = new Set();

  try {
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

      // Step 2: Process and write products immediately to avoid memory issues
      let categoryProductsCount = 0;

      // ⭐ Process products in batches of 5 to avoid memory exhaustion
      const BATCH_SIZE = 5;
      for (
        let batchStart = 0;
        batchStart < productList.length;
        batchStart += BATCH_SIZE
      ) {
        const batchEnd = Math.min(batchStart + BATCH_SIZE, productList.length);
        const batch = productList.slice(batchStart, batchEnd);

        console.log(
          `\n🔄 Processing batch ${
            Math.floor(batchStart / BATCH_SIZE) + 1
          }/${Math.ceil(productList.length / BATCH_SIZE)} (Products ${
            batchStart + 1
          }-${batchEnd}/${productList.length})`
        );

        // Open a fresh browser for each batch
        await retryPuppeteerWithProxyRotation(
          async (browser) => {
            for (let i = 0; i < batch.length; i++) {
              const basicProduct = batch[i];
              const globalIndex = batchStart + i;

              // ⭐ Check if product already processed (deduplication)
              if (processedProductIds.has(basicProduct.id)) {
                console.log(
                  `\n[${globalIndex + 1}/${
                    productList.length
                  }] ⏭️ Skipping duplicate: ${basicProduct.name} (${
                    basicProduct.id
                  })`
                );
                continue; // Skip duplicate product
              }

              console.log(
                `\n[${globalIndex + 1}/${productList.length}] Processing: ${
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

                // Add delay between products to avoid rate limiting
                await new Promise((resolve) => setTimeout(resolve, 2000)); // 2 second delay

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

                  // ⭐ Use color-specific URL and images (ONLY REAL DATA)
                  const variantUrl = colorVariant.colorUrl;
                  const variantImages = colorVariant.colorImages;
                  const variantImageUrl = variantImages[0];

                  // ⭐ Skip if no sizes - ONLY REAL DATA
                  if (sizes.length === 0) {
                    console.log(`  ⚠️ No sizes for color ${color}, skipping`);
                    continue; // Skip this color variant
                  }

                  // ⭐ Skip if missing URL or images - ONLY REAL DATA
                  if (
                    !variantUrl ||
                    !variantImages ||
                    variantImages.length === 0
                  ) {
                    console.log(
                      `  ⚠️ Missing data for color ${color}, skipping`
                    );
                    continue; // Skip this color variant
                  }

                  for (const sizeInfo of sizes) {
                    const size = sizeInfo.size;
                    const isInStock = sizeInfo.available;

                    variants.push({
                      price_currency: "USD",
                      original_price: originalPriceNum,
                      link_url: variantUrl, // ⭐ Color-specific URL
                      deeplink_url: variantUrl, // ⭐ Color-specific URL
                      image_url: variantImageUrl, // ⭐ Color-specific main image
                      alternate_image_urls: variantImages, // ⭐ Color-specific images
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
                      sale_price: 0,
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

                // ⭐ Write product IMMEDIATELY to disk (no memory buildup)
                try {
                  appendProductIncremental(inc, formattedProduct);
                  categoryProductsCount++;
                  totalProducts++;

                  console.log(
                    `  ✅ Added product with ${variants.length} variant(s) [Written to disk]`
                  );
                } catch (writeError) {
                  console.log(
                    `  ❌ Error writing product: ${writeError.message}`
                  );
                }

                // ⭐ Mark this product as processed to prevent duplicates
                processedProductIds.add(parentId);
              } catch (error) {
                console.log(`  ❌ Error processing product: ${error.message}`);
              }
            }

            // Browser will close automatically when function exits
            return true;
          },
          3,
          2000,
          "US"
        );

        console.log(
          `✅ Batch ${
            Math.floor(batchStart / BATCH_SIZE) + 1
          } completed, browser closed`
        );
      } // End of batch loop

      console.log(
        `✅ Completed ${category.name}: ${categoryProductsCount} products written`
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
    // ALWAYS finalize the catalog, even if there's an error
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
      // Return partial result
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
