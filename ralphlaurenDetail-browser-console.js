/**
 * Chrome Console Version - Extract Ralph Lauren Product Details
 *
 * Run this directly in the Chrome console when on a Ralph Lauren product page
 * OR use it to fetch a product page from any URL
 *
 * @format
 */

// ===== VERSION 1: Extract from CURRENT page (when you're on the product page) =====
const extractFromCurrentPage = () => {
  try {
    // Extract images
    const images = [];
    document
      .querySelectorAll(".swiper-zoom-container picture")
      .forEach((picture) => {
        const img = picture.querySelector("img")?.getAttribute("data-img");
        if (img) images.push(img);
      });

    // Extract sizes and availability
    const sizes = [];
    document
      .querySelectorAll(".js-attributes-list li.variations-attribute")
      .forEach((li) => {
        const size = li
          .querySelector("span.attribute-value bdi")
          ?.textContent.trim();
        const soldOut =
          li.querySelector(".sold-out")?.textContent.trim() || "Available";
        if (size) sizes.push({ size, availability: soldOut });
      });

    // Extract description - using the correct selectors we found
    const description = [];
    const selectors = [
      ".js-product-details .bullet-list ul li", // Main selector (in flyout)
      ".product-details-flyout .bullet-list ul li", // Alternative flyout selector
      ".flyout-body-inner .bullet-list ul li", // Flyout body
      ".flyout-body .bullet-list ul li", // Flyout body (less specific)
      ".rl-toaster-content .bullet-list ul li", // Toaster content
      ".bullet-list ul li", // Fallback: any bullet list
    ];

    for (const selector of selectors) {
      const bullets = document.querySelectorAll(selector);

      if (bullets.length > 0) {
        bullets.forEach((li) => {
          // Skip style number and navigation items
          if (
            li.classList.contains("style-number") ||
            li.classList.contains("js-extra") ||
            li.classList.contains("tree-item") ||
            li.textContent.trim().toLowerCase().includes("style number") ||
            li.querySelector("ul") // Skip items that have nested lists (navigation)
          ) {
            return; // Skip this element
          }

          const text = li.textContent.trim();
          // Only add substantial text that looks like product description
          if (text && text.length > 15 && !text.includes("\n\n\n")) {
            description.push(text);
          }
        });

        // If found descriptions, stop trying other selectors
        if (description.length > 0) {
          console.log(
            `✅ Found ${description.length} description items using selector: ${selector}`
          );
          break;
        }
      }
    }

    // Extract review count
    let reviewCount = document
      .querySelector(".bvseo-reviewCount")
      ?.textContent.trim();
    reviewCount = reviewCount ? parseInt(reviewCount) : 0;

    // Extract average rating
    let averageRating = document
      .querySelector(".bvseo-ratingValue")
      ?.textContent.trim();
    averageRating = averageRating ? parseFloat(averageRating) : 0;

    let ratingsCount = reviewCount;

    // Extract style number
    let styleNumber = document
      .querySelector(
        ".js-product-details .style-number span.screen-reader-digits"
      )
      ?.textContent.trim();

    if (!styleNumber) {
      styleNumber = document
        .querySelector(".style-number span.screen-reader-digits")
        ?.textContent.trim();
    }

    if (!styleNumber) {
      // Try to extract from page or from li with style-number class
      const styleElement = document.querySelector("li.style-number");
      if (styleElement) {
        const match = styleElement.textContent.match(/\d+/);
        if (match) styleNumber = match[0];
      }
    }

    const productData = {
      images,
      sizes,
      description,
      review_count: reviewCount,
      average_ratings: averageRating,
      ratings_count: ratingsCount,
      styleNumber,
    };

    console.log("📦 Product Data:", productData);
    return productData;
  } catch (error) {
    console.error("❌ Error:", error);
  }
};

// ===== VERSION 2: Fetch from URL (fetch any product page) =====
const fetchProductDetails = async (productUrl) => {
  try {
    console.log(`🔍 Fetching: ${productUrl}`);

    // Fetch the HTML
    const response = await fetch(productUrl, {
      headers: {
        accept:
          "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "accept-language": "en-AU,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
        "cache-control": "max-age=0",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "user-agent": navigator.userAgent,
      },
    });

    const html = await response.text();

    // Parse HTML into a DOM document
    const parser = new DOMParser();
    const doc = parser.parseFromString(html, "text/html");

    // Extract images
    const images = [];
    doc
      .querySelectorAll(".swiper-zoom-container picture")
      .forEach((picture) => {
        const img = picture.querySelector("img")?.getAttribute("data-img");
        if (img) images.push(img);
      });

    // Extract sizes and availability
    const sizes = [];
    doc
      .querySelectorAll(".js-attributes-list li.variations-attribute")
      .forEach((li) => {
        const size = li
          .querySelector("span.attribute-value bdi")
          ?.textContent.trim();
        const soldOut =
          li.querySelector(".sold-out")?.textContent.trim() || "Available";
        if (size) sizes.push({ size, availability: soldOut });
      });

    // Extract description - using the correct selectors
    const description = [];
    const selectors = [
      ".js-product-details .bullet-list ul li",
      ".product-details-flyout .bullet-list ul li",
      ".flyout-body-inner .bullet-list ul li",
      ".flyout-body .bullet-list ul li",
      ".rl-toaster-content .bullet-list ul li",
      ".bullet-list ul li",
    ];

    for (const selector of selectors) {
      const bullets = doc.querySelectorAll(selector);

      if (bullets.length > 0) {
        bullets.forEach((li) => {
          if (
            li.classList.contains("style-number") ||
            li.classList.contains("js-extra") ||
            li.classList.contains("tree-item") ||
            li.textContent.trim().toLowerCase().includes("style number") ||
            li.querySelector("ul")
          ) {
            return;
          }

          const text = li.textContent.trim();
          if (text && text.length > 15 && !text.includes("\n\n\n")) {
            description.push(text);
          }
        });

        if (description.length > 0) {
          console.log(
            `✅ Found ${description.length} description items using selector: ${selector}`
          );
          break;
        }
      }
    }

    // Extract review count
    let reviewCount = doc
      .querySelector(".bvseo-reviewCount")
      ?.textContent.trim();
    reviewCount = reviewCount ? parseInt(reviewCount) : 0;

    // Extract average rating
    let averageRating = doc
      .querySelector(".bvseo-ratingValue")
      ?.textContent.trim();
    averageRating = averageRating ? parseFloat(averageRating) : 0;

    let ratingsCount = reviewCount;

    // Extract style number
    let styleNumber = doc
      .querySelector(
        ".js-product-details .style-number span.screen-reader-digits"
      )
      ?.textContent.trim();

    if (!styleNumber) {
      styleNumber = doc
        .querySelector(".style-number span.screen-reader-digits")
        ?.textContent.trim();
    }

    if (!styleNumber) {
      const styleElement = doc.querySelector("li.style-number");
      if (styleElement) {
        const match = styleElement.textContent.match(/\d+/);
        if (match) styleNumber = match[0];
      }
    }

    const productData = {
      images,
      sizes,
      description,
      review_count: reviewCount,
      average_ratings: averageRating,
      ratings_count: ratingsCount,
      styleNumber,
    };

    console.log("📦 Product Data:", productData);
    return productData;
  } catch (error) {
    console.error("❌ Error fetching product:", error);
  }
};

// ===== USAGE INSTRUCTIONS =====
console.log("=".repeat(80));
console.log("🎯 RALPH LAUREN PRODUCT DETAIL EXTRACTOR");
console.log("=".repeat(80));
console.log("\n📋 Usage:\n");
console.log("1️⃣  Extract from CURRENT page (when on product page):");
console.log("   extractFromCurrentPage()");
console.log("\n2️⃣  Fetch from any product URL:");
console.log(
  '   await fetchProductDetails("https://www.ralphlauren.com/women-clothing-blazers/...")'
);
console.log("\n" + "=".repeat(80) + "\n");

// Make functions globally available
window.extractFromCurrentPage = extractFromCurrentPage;
window.fetchProductDetails = fetchProductDetails;

// Auto-run if on a product page
if (
  window.location.href.includes("ralphlauren.com") &&
  window.location.href.includes(".html")
) {
  console.log("🎯 Detected product page! Running extraction...\n");
  extractFromCurrentPage();
}
