/** @format */
const puppeteer = require("puppeteer");
const fs = require("fs");
const path = require("path");
const zlib = require("zlib");
const { v4: uuidv4, v5: uuidv5 } = require("uuid");

(async () => {
  const browser = await puppeteer.launch({
    headless: false,
    defaultViewport: false,
    userDataDir: "./tmp",
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
      "--disable-accelerated-2d-canvas",
      "--no-first-run",
      "--no-zygote",
      "--disable-gpu",
    ],
  });
  const page = await browser.newPage();

  // Target products for testing
  const TARGET_PRODUCTS = 1500;

  // Define sections to scrape
  const sections = [
    { path: "/en/women", gender: "Female" },
    { path: "/en/men", gender: "Male" },
  ];

  // Pagination loop through all pages for all sections
  const allProducts = [];
  const productsPerPage = 80;
  const maxPages = 1000; // Safety limit

  console.log(
    `🔄 Starting pagination loop for all sections (Target: ${TARGET_PRODUCTS} products)...`
  );

  // Loop through each section
  for (const section of sections) {
    console.log(
      `\n${"=".repeat(60)}\n📂 Processing section: ${section.path} (Gender: ${
        section.gender
      })\n${"=".repeat(60)}`
    );

    let startOffset = 0;
    let hasMoreProducts = true;

    while (hasMoreProducts && startOffset < maxPages * productsPerPage) {
      const pageUrl = `https://www.calvinklein.us${section.path}?sz=${productsPerPage}&start=${startOffset}`;
      console.log(`\n📄 Fetching page: ${pageUrl}`);
      console.log(`   Start offset: ${startOffset}`);

      try {
        await page.goto(pageUrl, {
          waitUntil: "domcontentloaded",
          timeout: 30000,
        });

        // Wait for products to load (with timeout)
        try {
          await page.waitForSelector(".product", { timeout: 10000 });
        } catch (e) {
          console.log(
            "⚠️ No products found on this page. Stopping pagination."
          );
          hasMoreProducts = false;
          break;
        }

        // Load all products on current page (scroll and click Load More button)
        console.log("   🔄 Loading all products on this page...");
        let pagePreviousProductCount = 0;
        let pageScrollAttempts = 0;
        let pageNoNewProductsCount = 0;
        const maxPageScrolls = 100;

        while (pageScrollAttempts < maxPageScrolls) {
          // Get current product count on this page
          const pageCurrentProductCount = await page.evaluate(() => {
            return document.querySelectorAll(".product").length;
          });

          // Check if "Load More" button is visible
          const loadMoreButton = await page.evaluate(() => {
            const button = document.querySelector(".ds-load-more, .load-more");
            if (button) {
              const style = window.getComputedStyle(button);
              const rect = button.getBoundingClientRect();
              return {
                exists: true,
                visible:
                  style.display !== "none" &&
                  style.visibility !== "hidden" &&
                  rect.height > 0,
                text: button.textContent.trim(),
              };
            }
            return { exists: false, visible: false, text: "" };
          });

          // If Load More button is visible, click it
          if (loadMoreButton.exists && loadMoreButton.visible) {
            console.log(`      🔘 Found 'Load More' button, clicking...`);
            try {
              // Scroll to button to make sure it's in view
              await page.evaluate(() => {
                const button = document.querySelector(
                  ".ds-load-more, .load-more"
                );
                if (button) {
                  button.scrollIntoView({
                    behavior: "smooth",
                    block: "center",
                  });
                }
              });

              await new Promise((resolve) => setTimeout(resolve, 500));

              // Click the button
              await page.click(".ds-load-more, .load-more");
              console.log(
                `      ✅ Clicked 'Load More' button, waiting for products...`
              );

              // Wait for new products to load
              await new Promise((resolve) => setTimeout(resolve, 3000));

              // Re-check product count after clicking
              const countAfterClick = await page.evaluate(() => {
                return document.querySelectorAll(".product").length;
              });

              if (countAfterClick > pageCurrentProductCount) {
                console.log(
                  `      📦 Loaded ${
                    countAfterClick - pageCurrentProductCount
                  } new products (Total on page: ${countAfterClick})`
                );
                pagePreviousProductCount = countAfterClick;
                pageNoNewProductsCount = 0;
                pageScrollAttempts++;
                continue;
              }
            } catch (error) {
              console.log(
                `      ⚠️ Error clicking Load More button: ${error.message}`
              );
            }
          }

          // If no new products loaded after 3 attempts and no Load More button, stop
          if (
            pageCurrentProductCount === pagePreviousProductCount &&
            !loadMoreButton.visible
          ) {
            pageNoNewProductsCount++;
            if (pageNoNewProductsCount >= 3) {
              console.log(
                `      ✅ All products loaded on this page (${pageCurrentProductCount} products)`
              );
              break;
            }
          } else if (pageCurrentProductCount > pagePreviousProductCount) {
            pageNoNewProductsCount = 0; // Reset counter if new products found
            console.log(
              `      📦 Scroll attempt ${
                pageScrollAttempts + 1
              }: ${pageCurrentProductCount} products on page`
            );
          }

          pagePreviousProductCount = pageCurrentProductCount;

          // Scroll to bottom gradually
          await page.evaluate(() => {
            window.scrollBy(0, window.innerHeight);
          });

          // Small delay before scrolling to bottom
          await new Promise((resolve) => setTimeout(resolve, 500));

          // Scroll to absolute bottom
          await page.evaluate(() => {
            window.scrollTo(0, document.body.scrollHeight);
          });

          // Wait for potential new content to load
          await new Promise((resolve) => setTimeout(resolve, 2000));

          pageScrollAttempts++;
        }

        // Extract products from current page (after loading all products)
        const pageProducts = await page.evaluate((gender) => {
          const productElements = document.querySelectorAll(".product");
          const results = [];

          productElements.forEach((productEl) => {
            try {
              // Extract all colors from swatch items
              const colorSwatches = productEl.querySelectorAll(".swatch-item");
              const colors = Array.from(colorSwatches)
                .map((swatch) => {
                  return swatch.getAttribute("aria-label") || "";
                })
                .filter((color) => color);

              // Extract sale price
              const salePriceEl = productEl.querySelector(".sales .value");
              const salePrice = salePriceEl
                ? salePriceEl.getAttribute("content") ||
                  salePriceEl.textContent.trim()
                : "";

              // Extract original/discount price
              const originalPriceEl = productEl.querySelector(
                ".ds-slash-price .value"
              );
              const originalPrice = originalPriceEl
                ? originalPriceEl.getAttribute("content") ||
                  originalPriceEl.textContent.trim()
                : "";

              // Extract product URL
              const productLink = productEl.querySelector(
                ".ds-product-name a, .pdp-link a"
              );
              const productUrl = productLink
                ? productLink.getAttribute("href") || productLink.href
                : "";

              // Brand name
              const brandName = "Calvin Klein";

              // Extract rating (from data-starrating or count filled stars)
              let rating = "0";
              const ratingContainer =
                productEl.querySelector("[data-starrating]");
              if (ratingContainer) {
                rating = ratingContainer.getAttribute("data-starrating") || "0";
              } else {
                // Fallback: count filled stars
                const filledStars = productEl.querySelectorAll(
                  ".ds-star-rating .icon-star-100"
                ).length;
                if (filledStars > 0) {
                  rating = filledStars.toString();
                }
              }

              // Extract rating count
              const ratingCountEl =
                productEl.querySelector(".ds-counter-rating");
              let ratingCount = "0";
              if (ratingCountEl) {
                const countText = ratingCountEl.textContent.trim();
                const match = countText.match(/\((\d+)\)/);
                ratingCount = match ? match[1] : "0";
              }

              // Helper function to get high-resolution image URL (wid=1200)
              function getFullResImageUrl(url) {
                if (!url || !url.includes("calvinklein.scene7.com")) {
                  return url; // Return as-is if not a Scene7 URL
                }
                try {
                  const urlObj = new URL(url);
                  // Set wid=1200 for high resolution, remove hei
                  urlObj.searchParams.set("wid", "1200");
                  urlObj.searchParams.delete("hei");
                  return urlObj.toString();
                } catch (e) {
                  // Fallback: replace wid with 1200, remove hei
                  return url
                    .replace(/[?&]wid=\d+/gi, "&wid=1200")
                    .replace(/[?&]hei=\d+(&|$)/gi, "$1")
                    .replace(/^([^?]*)\?&/, "$1?");
                }
              }

              // Extract image URL from data-product-image-attr
              let imageUrl = "";
              let alternativeImages = [];
              const imageContainer = productEl.querySelector(
                "[data-product-image-attr]"
              );
              if (imageContainer) {
                try {
                  const attrValue = imageContainer.getAttribute(
                    "data-product-image-attr"
                  );
                  if (attrValue) {
                    const imageData = JSON.parse(attrValue);
                    if (imageData.product && imageData.product.images) {
                      const images = imageData.product.images;
                      if (images.length > 0) {
                        // Get base URL and convert to full resolution
                        const baseUrl = images[0].absURL || images[0].url || "";
                        imageUrl = getFullResImageUrl(baseUrl);

                        // Process alternative images
                        alternativeImages = images
                          .slice(1)
                          .map((img) => {
                            const altUrl = img.absURL || img.url || "";
                            return getFullResImageUrl(altUrl);
                          })
                          .filter((url) => url && url !== imageUrl);
                      }
                    }
                  }
                } catch (e) {
                  // If JSON parse fails, fallback to img elements
                }
              }

              // Fallback: extract from img src if no data-product-image-attr found
              if (!imageUrl) {
                const firstImg = productEl.querySelector(
                  ".plp-slick-img:not([src*='data:image'])"
                );
                if (
                  firstImg &&
                  firstImg.src &&
                  !firstImg.src.includes("data:image")
                ) {
                  imageUrl = getFullResImageUrl(firstImg.src);
                }
              }

              // Get alternative images from remaining img elements if needed
              if (alternativeImages.length === 0) {
                const allImgs = productEl.querySelectorAll(".plp-slick-img");
                alternativeImages = Array.from(allImgs)
                  .map((img) => {
                    const imgSrc = img.src || "";
                    return getFullResImageUrl(imgSrc);
                  })
                  .filter(
                    (url) =>
                      url && !url.includes("data:image") && url !== imageUrl
                  );
              }

              results.push({
                productUrl,
                brandName,
                colors,
                salePrice,
                originalPrice,
                rating,
                ratingCount,
                imageUrl,
                alternativeImages,
                gender: gender, // Add gender based on section
              });
            } catch (error) {
              console.error("Error extracting product:", error);
            }
          });

          return results;
        }, section.gender);

        // Check if we got products from this page
        if (pageProducts && pageProducts.length > 0) {
          console.log(`✅ Extracted ${pageProducts.length} products from page`);
          allProducts.push(...pageProducts);
          console.log(`📊 Total products so far: ${allProducts.length}`);

          // Check if we've reached target products
          if (allProducts.length >= TARGET_PRODUCTS) {
            console.log(
              `\n🎯 Reached target of ${TARGET_PRODUCTS} products. Stopping pagination.`
            );
            // Limit to target products
            allProducts.length = TARGET_PRODUCTS;
            hasMoreProducts = false;
            break; // Break from inner pagination loop
          }

          // Move to next page
          startOffset += productsPerPage;

          // Add delay between page requests
          await new Promise((resolve) => setTimeout(resolve, 1000));
        } else {
          console.log(
            "⚠️ No products found on this page. Stopping pagination."
          );
          hasMoreProducts = false;
          break;
        }
      } catch (error) {
        console.error(
          `❌ Error fetching page at offset ${startOffset}:`,
          error.message
        );
        hasMoreProducts = false;
        break;
      }

      // Check if we've reached target products after finishing this while loop iteration
      if (allProducts.length >= TARGET_PRODUCTS) {
        console.log(
          `\n🎯 Reached target of ${TARGET_PRODUCTS} products. Stopping all sections.`
        );
        // Limit to target products
        allProducts.length = TARGET_PRODUCTS;
        hasMoreProducts = false; // Stop the while loop
        break; // Break from outer section loop
      }
    } // End of while loop

    // Check if we've reached target products after finishing this section
    if (allProducts.length >= TARGET_PRODUCTS) {
      console.log(
        `\n🎯 Reached target of ${TARGET_PRODUCTS} products. Stopping all sections.`
      );
      // Limit to target products
      allProducts.length = TARGET_PRODUCTS;
      break; // Break from outer section loop
    }
  } // End of for loop

  console.log(
    `\n✅ Finished pagination. Total products extracted: ${allProducts.length}\n`
  );

  // Process each product detail page to get full information
  console.log(
    `\n🔍 Processing product details (Max: ${TARGET_PRODUCTS} products)...`
  );
  const processedProducts = [];
  const productsToProcess = Math.min(allProducts.length, TARGET_PRODUCTS);

  for (let i = 0; i < productsToProcess; i++) {
    const product = allProducts[i];
    if (!product.productUrl) {
      console.log(`⚠️ Skipping product ${i + 1}: No product URL`);
      continue;
    }

    try {
      // Make URL absolute if relative
      const productUrl = product.productUrl.startsWith("http")
        ? product.productUrl
        : `https://www.calvinklein.us${product.productUrl}`;

      console.log(
        `\n📦 Processing product ${i + 1}/${productsToProcess}: ${productUrl}`
      );

      // Open product in new tab
      const productPage = await browser.newPage();
      await productPage.goto(productUrl, {
        waitUntil: "domcontentloaded",
        timeout: 30000,
      });

      // Wait for product page to load
      try {
        await productPage.waitForSelector(".variant-list", { timeout: 10000 });
      } catch (e) {
        console.log(`   ⚠️ Product page not fully loaded, skipping...`);
        await productPage.close();
        continue;
      }

      // Extract product name and details (same for all colors)
      const productDetails = await productPage.evaluate(() => {
        let productName = "";
        let description = "";
        let materials = "";
        let details = [];
        let productId = "";

        // Extract product name
        const nameElement = document.querySelector(
          "h1.product-name, .product-title, h1[itemprop='name']"
        );
        if (nameElement) {
          productName = nameElement.textContent.trim();
        }

        // Extract product ID from URL or data attributes
        const pidMatch = window.location.pathname.match(/\/([A-Z0-9-]+)\.html/);
        if (pidMatch) {
          productId = pidMatch[1];
        }

        // Extract description from "About" section
        const aboutSection = document.querySelector(
          ".content-block .content-description"
        );
        if (aboutSection) {
          description = aboutSection.textContent.trim();
        }

        // Extract details list
        const detailsList = document.querySelectorAll(
          ".content-list .content-list-item"
        );
        details = Array.from(detailsList).map((item) =>
          item.textContent.trim()
        );

        // Extract materials/composition from content-table
        const contentRows = document.querySelectorAll(
          ".content-table .content-row"
        );
        contentRows.forEach((row) => {
          const columns = row.querySelectorAll(".content-column");
          if (columns.length === 2) {
            const label = columns[0].textContent.trim();
            const value = columns[1].textContent.trim();
            if (label.toLowerCase().includes("composition")) {
              materials = value;
            }
          }
        });

        return { productName, productId, description, materials, details };
      });

      // Extract all colors from the product page
      const colorVariants = await productPage.evaluate(() => {
        const colors = [];
        const colorInputs = document.querySelectorAll(
          "#colorscolorCode .variant-color-item"
        );
        colorInputs.forEach((input) => {
          const label = input
            .closest(".variant-list__item")
            ?.querySelector("label span[aria-label]");
          if (label) {
            const colorName = label.getAttribute("aria-label") || "";
            const colorCode = input.getAttribute("data-attr-value") || "";
            const url = input.getAttribute("data-url") || "";
            colors.push({ colorName, colorCode, url });
          }
        });
        return colors;
      });

      console.log(`   🎨 Found ${colorVariants.length} colors`);

      // Process each color
      const colorDetails = [];
      for (const color of colorVariants) {
        try {
          console.log(`   🎨 Processing color: ${color.colorName}`);

          // Click on the color to load its variants
          try {
            // Try multiple selectors for the color input/label
            const selectors = [
              `input[data-attr-value="${color.colorCode}"].variant-color-item`,
              `label[for*="${color.colorCode}"]`,
              `span[aria-label="${color.colorName}"]`,
              `label:has(span[aria-label="${color.colorName}"])`,
            ];

            let clicked = false;
            for (const selector of selectors) {
              try {
                const element = await productPage.$(selector);
                if (element) {
                  await element.click();
                  clicked = true;
                  break;
                }
              } catch (e) {
                // Try next selector
              }
            }

            if (!clicked) {
              // Try using evaluate to click via JavaScript
              await productPage.evaluate((colorCode) => {
                const input = document.querySelector(
                  `input[data-attr-value="${colorCode}"].variant-color-item`
                );
                if (input) {
                  input.click();
                  return true;
                }
                const label = document.querySelector(
                  `label[for*="${colorCode}"]`
                );
                if (label) {
                  label.click();
                  return true;
                }
                return false;
              }, color.colorCode);
            }

            await new Promise((resolve) => setTimeout(resolve, 3000)); // Wait for variant to load
          } catch (e) {
            console.log(
              `      ⚠️ Could not click color ${color.colorName}: ${e.message}`
            );
          }

          // Wait a bit more for images to load after color change
          await new Promise((resolve) => setTimeout(resolve, 1000));

          // Extract sizes and availability for this color (regular sizes, waist, length)
          const sizeInfo = await productPage.evaluate(() => {
            const variants = {
              sizes: [],
              waist: [],
              length: [],
            };

            // Extract regular sizes
            const sizeInputs = document.querySelectorAll(
              "#sizessize .variant-size"
            );
            sizeInputs.forEach((input) => {
              const label = document.querySelector(`label[for="${input.id}"]`);
              if (label) {
                const size = input.getAttribute("data-attr-value") || "";
                const isEnabled = label.classList.contains("size-enabled");
                const isDisabled = label.classList.contains("size-disabled");
                variants.sizes.push({
                  size,
                  inStock: isEnabled && !isDisabled,
                });
              }
            });

            // Extract waist sizes
            const waistInputs = document.querySelectorAll(
              "#sizeswaist .variant-waist"
            );
            waistInputs.forEach((input) => {
              const label = document.querySelector(`label[for="${input.id}"]`);
              if (label) {
                const waist = input.getAttribute("data-attr-value") || "";
                const isEnabled = label.classList.contains("size-enabled");
                const isDisabled = label.classList.contains("size-disabled");
                variants.waist.push({
                  waist,
                  inStock: isEnabled && !isDisabled,
                });
              }
            });

            // Extract length options
            const lengthInputs = document.querySelectorAll(
              "#sizeslength .variant-length"
            );
            lengthInputs.forEach((input) => {
              const label = document.querySelector(`label[for="${input.id}"]`);
              if (label) {
                const length = input.getAttribute("data-attr-value") || "";
                const lengthLabel =
                  label.getAttribute("aria-label") || label.textContent.trim();
                const isEnabled = label.classList.contains("size-enabled");
                const isDisabled = label.classList.contains("size-disabled");
                variants.length.push({
                  length,
                  lengthLabel,
                  inStock: isEnabled && !isDisabled,
                });
              }
            });

            return variants;
          });

          // Extract images for this color - only from product image slider, not recommendations
          // Scroll through product image gallery to load lazy images
          await productPage.evaluate(() => {
            // Find the main product image slider (not recommendation section)
            const productImageSlider = document.querySelector(
              ".product-image.swiper-slide, .swiper-wrapper:not(.product-grid) .swiper-slide"
            );
            if (productImageSlider) {
              const swiperContainer = productImageSlider.closest(".swiper");
              if (swiperContainer) {
                // Scroll through the slider to load all images
                const slides =
                  swiperContainer.querySelectorAll(".swiper-slide");
                slides.forEach((slide, index) => {
                  if (index > 0) {
                    slide.scrollIntoView({
                      behavior: "smooth",
                      block: "nearest",
                    });
                  }
                });
              }
            }
          });
          await new Promise((resolve) => setTimeout(resolve, 1500));

          const colorImages = await productPage.evaluate(() => {
            // Helper function to get high-resolution image URL (wid=1200)
            function getFullResImageUrl(url) {
              if (!url || !url.includes("calvinklein.scene7.com")) {
                return url;
              }
              try {
                const urlObj = new URL(url);
                // Set wid=1200 for high resolution, remove hei
                urlObj.searchParams.set("wid", "1200");
                urlObj.searchParams.delete("hei");
                return urlObj.toString();
              } catch (e) {
                // Fallback: replace wid with 1200, remove hei
                return url
                  .replace(/[?&]wid=\d+/gi, "&wid=1200")
                  .replace(/[?&]hei=\d+(&|$)/gi, "$1")
                  .replace(/^([^?]*)\?&/, "$1?");
              }
            }

            const images = [];
            const seenUrls = new Set();

            // Only get images from the main product image slider
            // Exclude recommendation sections (product-recommendation, einstein-recommendation, etc.)
            // Target only .product-image.swiper-slide (main product gallery)
            const productImageSlides = document.querySelectorAll(
              ".product-image.swiper-slide picture"
            );

            // Also exclude any images from recommendation sections
            const recommendationSections = document.querySelectorAll(
              ".product-recommendation, .einstein-recommendation, .product-recommendation__tile-wrapper, .ctl-carousel"
            );
            const excludedElements = new Set();
            recommendationSections.forEach((section) => {
              section
                .querySelectorAll("picture, img")
                .forEach((el) => excludedElements.add(el));
            });

            productImageSlides.forEach((picture) => {
              // Skip if this picture is in a recommendation section
              if (excludedElements.has(picture)) {
                return;
              }

              // Check if picture is inside a recommendation section by checking parent
              const isInRecommendation = picture.closest(
                ".product-recommendation, .einstein-recommendation, .product-recommendation__tile-wrapper, .ctl-carousel, .product-grid"
              );
              if (isInRecommendation) {
                return;
              }

              // Check img tag inside picture
              const img = picture.querySelector("img");
              if (img && !excludedElements.has(img)) {
                const src =
                  img.src ||
                  img.getAttribute("data-src") ||
                  img.getAttribute("data-lazy-src") ||
                  img.getAttribute("data-original");
                if (
                  src &&
                  !src.includes("data:image") &&
                  !src.includes("base64") &&
                  src.includes("calvinklein.scene7.com") &&
                  !src.includes("/Nav/") &&
                  !src.includes("Nav_")
                ) {
                  const fullResUrl = getFullResImageUrl(src);
                  if (fullResUrl && !seenUrls.has(fullResUrl)) {
                    seenUrls.add(fullResUrl);
                    images.push(fullResUrl);
                  }
                }
              }

              // Check source tags inside picture
              const sources = picture.querySelectorAll("source");
              sources.forEach((source) => {
                const srcset = source.getAttribute("srcset") || "";
                if (srcset) {
                  const urls = srcset
                    .split(",")
                    .map((s) => s.trim().split(" ")[0]);
                  urls.forEach((url) => {
                    if (
                      url &&
                      !url.includes("data:image") &&
                      url.includes("calvinklein.scene7.com") &&
                      !url.includes("/Nav/") &&
                      !url.includes("Nav_")
                    ) {
                      const fullResUrl = getFullResImageUrl(url);
                      if (!seenUrls.has(fullResUrl)) {
                        seenUrls.add(fullResUrl);
                        images.push(fullResUrl);
                      }
                    }
                  });
                }
              });
            });

            // Also try to get images from data-product-image-attr in the main product area
            // But exclude recommendation sections
            const mainProductArea = document.querySelector(
              ".product-detail, .product-main, .product-content"
            );
            if (mainProductArea) {
              const imageContainer = mainProductArea.querySelector(
                "[data-product-image-attr]"
              );
              if (imageContainer) {
                try {
                  const attrValue = imageContainer.getAttribute(
                    "data-product-image-attr"
                  );
                  if (attrValue) {
                    const imageData = JSON.parse(attrValue);
                    if (imageData.product && imageData.product.images) {
                      const imgArray = imageData.product.images;
                      imgArray.forEach((img) => {
                        const url = img.absURL || img.url || "";
                        if (
                          url &&
                          url.includes("calvinklein.scene7.com") &&
                          !url.includes("/Nav/") &&
                          !url.includes("Nav_")
                        ) {
                          const fullResUrl = getFullResImageUrl(url);
                          if (!seenUrls.has(fullResUrl)) {
                            seenUrls.add(fullResUrl);
                            images.push(fullResUrl);
                          }
                        }
                      });
                    }
                  }
                } catch (e) {
                  // Ignore parsing errors
                }
              }
            }

            return images;
          });

          colorDetails.push({
            colorName: color.colorName,
            colorCode: color.colorCode,
            sizes: sizeInfo.sizes,
            waist: sizeInfo.waist,
            length: sizeInfo.length,
            images: colorImages,
          });

          const totalVariants =
            sizeInfo.sizes.length +
            sizeInfo.waist.length +
            sizeInfo.length.length;
          console.log(
            `      ✅ Color ${color.colorName}: ${sizeInfo.sizes.length} sizes, ${sizeInfo.waist.length} waist sizes, ${sizeInfo.length.length} length options, ${colorImages.length} images`
          );
        } catch (error) {
          console.log(
            `      ⚠️ Error processing color ${color.colorName}: ${error.message}`
          );
        }
      }

      // Combine all product information
      const processedProduct = {
        ...product,
        productName:
          productDetails.productName ||
          product.productUrl.split("/").pop().replace(".html", ""),
        productId:
          productDetails.productId ||
          product.productUrl.split("/").pop().replace(".html", ""),
        description: productDetails.description,
        materials: productDetails.materials,
        details: productDetails.details,
        colorDetails: colorDetails,
      };

      processedProducts.push(processedProduct);
      await productPage.close();

      // Add delay between product requests
      await new Promise((resolve) => setTimeout(resolve, 1000));
    } catch (error) {
      console.error(`❌ Error processing product ${i + 1}: ${error.message}`);
    }
  }

  console.log(
    `\n✅ Finished processing all products. Total processed: ${processedProducts.length}\n`
  );

  // Transform products to Marshalls format
  console.log("\n🔄 Transforming products to catalog format...");

  function parsePrice(priceString) {
    if (!priceString) return 0;
    // Remove currency symbols and commas, extract number
    const cleaned = priceString.replace(/[$,]/g, "").trim();
    const match = cleaned.match(/(\d+\.?\d*)/);
    return match ? parseFloat(match[1]) : 0;
  }

  let variantIdCounter = 0;
  function generateVariantId() {
    variantIdCounter++;
    return `${Date.now()}-${variantIdCounter}-${Math.random()
      .toString(36)
      .substr(2, 9)}`;
  }

  function normalizeColorName(colorName) {
    if (!colorName) return "";
    // Convert to lowercase, replace spaces and special characters with hyphens
    return colorName
      .toLowerCase()
      .trim()
      .replace(/\s+/g, "-")
      .replace(/[^a-z0-9-]/g, "")
      .replace(/-+/g, "-")
      .replace(/^-|-$/g, "");
  }

  function ensureHighResImage(url) {
    if (!url || !url.includes("calvinklein.scene7.com")) {
      return url;
    }
    try {
      const urlObj = new URL(url);
      // Set wid=1200 for high resolution, remove hei
      urlObj.searchParams.set("wid", "1200");
      urlObj.searchParams.delete("hei");
      return urlObj.toString();
    } catch (e) {
      // Fallback: replace wid with 1200, remove hei
      return url
        .replace(/[?&]wid=\d+/gi, "&wid=1200")
        .replace(/[?&]hei=\d+(&|$)/gi, "$1")
        .replace(/^([^?]*)\?&/, "$1?");
    }
  }

  const formattedProducts = processedProducts.map((product) => {
    // Parse prices
    const originalPrice = parsePrice(product.originalPrice);
    const salePrice = parsePrice(product.salePrice);
    const finalPrice =
      salePrice > 0 && salePrice < originalPrice ? salePrice : originalPrice;
    const isOnSale = salePrice > 0 && salePrice < originalPrice;
    const discount =
      isOnSale && originalPrice > 0
        ? Math.round(((originalPrice - salePrice) / originalPrice) * 100)
        : 0;

    // Make product URL absolute
    let absoluteProductUrl = product.productUrl;
    if (!absoluteProductUrl.startsWith("http")) {
      absoluteProductUrl = `https://www.calvinklein.us${absoluteProductUrl}`;
    }

    // Extract product ID from URL or use productId
    const productIdMatch = absoluteProductUrl.match(/\/([A-Z0-9-]+)\.html/);
    const parentProductId = productIdMatch
      ? productIdMatch[1]
      : product.productId || "";

    // Extract category from URL
    const urlParts = absoluteProductUrl.split("/");
    const categoryIndex = urlParts.findIndex((part) => part === "apparel");
    const category =
      categoryIndex >= 0 && urlParts[categoryIndex + 1]
        ? urlParts[categoryIndex + 1]
            .replace(/-/g, " ")
            .replace(/\b\w/g, (l) => l.toUpperCase())
        : "Clothing";

    // Create variants from colorDetails
    const variants = [];

    product.colorDetails.forEach((colorDetail) => {
      const colorName = colorDetail.colorName;
      const colorCode = colorDetail.colorCode;
      const images = colorDetail.images || [];

      // Filter to only Scene7 product images (not navigation images)
      // Only include images from calvinklein.scene7.com that are product images
      const productImages = images.filter((img) => {
        if (!img || typeof img !== "string") return false;
        // Must be from Scene7 domain
        if (!img.includes("calvinklein.scene7.com")) return false;
        // Exclude navigation images (Nav folder) and other non-product images
        if (img.includes("/Nav/") || img.includes("Nav_")) return false;
        // Must look like a product image (has product code pattern)
        return true;
      });

      // Get primary image (first valid product image, or fallback) and ensure high resolution
      let primaryImage = "";
      if (productImages.length > 0) {
        primaryImage = ensureHighResImage(productImages[0]);
      } else if (
        product.imageUrl &&
        product.imageUrl.includes("calvinklein.scene7.com")
      ) {
        primaryImage = ensureHighResImage(product.imageUrl);
      }

      // Get alternate images (max 5, excluding primary image) and ensure high resolution
      const alternateImages = productImages
        .filter((img) => img !== productImages[0]) // Filter by original URL before processing
        .slice(0, 5)
        .map((img) => ensureHighResImage(img));

      // Create MPN (Manufacturer Part Number) - only parentProductId + normalized color name
      const normalizedColorName = normalizeColorName(colorName);
      const mpn = uuidv5(
        `${parentProductId}-${normalizedColorName}`,
        "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
      );

      // Handle regular sizes
      if (colorDetail.sizes && colorDetail.sizes.length > 0) {
        colorDetail.sizes.forEach((sizeInfo) => {
          variants.push({
            price_currency: "USD",
            original_price: originalPrice,
            link_url: absoluteProductUrl,
            deeplink_url: absoluteProductUrl,
            image_url: primaryImage,
            alternate_image_urls: alternateImages,
            is_on_sale: isOnSale,
            is_in_stock: sizeInfo.inStock,
            size: sizeInfo.size,
            color: colorName,
            mpn: mpn,
            ratings_count: parseInt(product.ratingCount) || 0,
            average_ratings: parseFloat(product.rating) || 0,
            review_count: parseInt(product.ratingCount) || 0,
            selling_price: finalPrice,
            sale_price: salePrice,
            final_price: finalPrice,
            discount: discount,
            operation_type: "INSERT",
            variant_id: uuidv5(
              `${parentProductId}-${normalizedColorName}-${sizeInfo.size}`,
              "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
            ),
            variant_description: "",
          });
        });
      }

      // Handle waist sizes (for jeans/pants)
      if (colorDetail.waist && colorDetail.waist.length > 0) {
        // If there are length options, create variants for each waist/length combination
        if (colorDetail.length && colorDetail.length.length > 0) {
          colorDetail.waist.forEach((waistInfo) => {
            colorDetail.length.forEach((lengthInfo) => {
              variants.push({
                price_currency: "USD",
                original_price: originalPrice,
                link_url: absoluteProductUrl,
                deeplink_url: absoluteProductUrl,
                image_url: primaryImage,
                alternate_image_urls: alternateImages,
                is_on_sale: isOnSale,
                is_in_stock: waistInfo.inStock && lengthInfo.inStock,
                size: `${waistInfo.waist} ${
                  lengthInfo.lengthLabel || lengthInfo.length
                }`,
                color: colorName,
                mpn: mpn,
                ratings_count: parseInt(product.ratingCount) || 0,
                average_ratings: parseFloat(product.rating) || 0,
                review_count: parseInt(product.ratingCount) || 0,
                selling_price: finalPrice,
                sale_price: salePrice,
                final_price: finalPrice,
                discount: discount,
                operation_type: "INSERT",
                variant_id: uuidv5(
                  `${parentProductId}-${normalizedColorName}-${waistInfo.waist} ${lengthInfo.length}`,
                  "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
                ),
                variant_description: "",
              });
            });
          });
        } else {
          // Only waist sizes, no length options
          colorDetail.waist.forEach((waistInfo) => {
            variants.push({
              price_currency: "USD",
              original_price: originalPrice,
              link_url: absoluteProductUrl,
              deeplink_url: absoluteProductUrl,
              image_url: primaryImage,
              alternate_image_urls: alternateImages,
              is_on_sale: isOnSale,
              is_in_stock: waistInfo.inStock,
              size: waistInfo.waist.toString(),
              color: colorName,
              mpn: mpn,
              ratings_count: parseInt(product.ratingCount) || 0,
              average_ratings: parseFloat(product.rating) || 0,
              review_count: parseInt(product.ratingCount) || 0,
              selling_price: finalPrice,
              sale_price: salePrice,
              final_price: finalPrice,
              discount: discount,
              operation_type: "INSERT",
              variant_id: uuidv5(
                `${parentProductId}-${normalizedColorName}-${waistInfo.waist}`,
                "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
              ),
              variant_description: "",
            });
          });
        }
      }

      // If no sizes at all, create at least one variant for the color
      if (!colorDetail.sizes?.length && !colorDetail.waist?.length) {
        variants.push({
          price_currency: "USD",
          original_price: originalPrice,
          link_url: absoluteProductUrl,
          deeplink_url: absoluteProductUrl,
          image_url: primaryImage,
          alternate_image_urls: alternateImages,
          is_on_sale: isOnSale,
          is_in_stock: true,
          size: "",
          color: colorName,
          mpn: mpn,
          ratings_count: parseInt(product.ratingCount) || 0,
          average_ratings: parseFloat(product.rating) || 0,
          review_count: parseInt(product.ratingCount) || 0,
          selling_price: finalPrice,
          sale_price: salePrice,
          final_price: finalPrice,
          discount: discount,
          operation_type: "INSERT",
          variant_id: uuidv5(
            `${parentProductId}-${normalizedColorName}`,
            "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
          ),
          variant_description: "",
        });
      }
    });

    return {
      parent_product_id: parentProductId,
      name: product.productName || parentProductId,
      description: product.description || "",
      category: category,
      retailer_domain: "calvinklein.us",
      brand: product.brandName || "Calvin Klein",
      gender: product.gender || "Female", // Use gender from product (set based on section)
      materials: product.materials || "",
      return_policy_link: "",
      return_policy: "",
      size_chart: "",
      available_bank_offers: "",
      available_coupons: "",
      variants: variants,
      operation_type: "INSERT",
      source: "calvinklein",
    };
  });

  // Create catalog data object in Marshalls format
  const catalogData = {
    store_info: {
      name: "Calvin Klein",
      domain: "calvinklein.us",
      currency: "USD",
      country: "US",
      total_products: formattedProducts.length,
      categories: ["Clothing"],
      crawled_at: new Date().toISOString(),
    },
    products: formattedProducts,
  };

  // Save products to files
  const outputDir = path.join(__dirname, "output", "US");
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }

  // Create directory name matching Marshalls format
  const dirPath = path.join(outputDir, "calvinklein-US");

  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }

  // Create JSON file
  const jsonFilePath = path.join(dirPath, "catalog.json");
  fs.writeFileSync(jsonFilePath, JSON.stringify(catalogData, null, 2), "utf8");
  console.log(`JSON file generated: ${jsonFilePath}`);

  // Create JSONL file (each product on a separate line)
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

  await browser.close();
})();
