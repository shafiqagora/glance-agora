/** @format */

const fs = require("fs");
const path = require("path");
const { v5: uuidv5 } = require("uuid");
const cheerio = require("cheerio");

const COLOR_NAMESPACE = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
const VARIANT_NAMESPACE = "6ba7b810-9dad-11d1-80b4-00c04fd430c1";

const STORE_DATA = {
  name: "Marshalls",
  domain: "marshalls.com",
  currency: "USD",
  country: "US",
  return_policy_link:
    "https://www.marshalls.com/us/store/jump/topic/returns/2400118",
  return_policy:
    "Marshalls accepts returns within 30 days of purchase with original receipt.",
};

const PRODUCT_URLS = [
  "https://www.marshalls.com/us/store/products/women-women-clothing/_/N-3255077828?Nr=AND%28isEarlyAccess%3Afalse%2COR%28product.catalogId%3Atjmaxx%29%2Cproduct.siteId%3Amarshalls%29",
  "https://www.marshalls.com/us/store/products/men-men-clothing/_/N-2085788411?Nr=AND%28isEarlyAccess%3Afalse%2COR%28product.catalogId%3Atjmaxx%29%2Cproduct.siteId%3Amarshalls%29",
];

const WAIT_BETWEEN_REQUESTS = 500;

const HEADERS = {
  Accept: "text/html, */*; q=0.01",
  "Accept-Language": "en-US,en;q=0.9",
  "Accept-Encoding": "gzip, deflate, br, zstd",
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
  "sec-ch-ua":
    '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
  "sec-ch-ua-mobile": "?0",
  "sec-ch-ua-platform": '"Windows"',
  "sec-fetch-dest": "empty",
  "sec-fetch-mode": "cors",
  "sec-fetch-site": "same-origin",
  "x-requested-with": "XMLHttpRequest",
  adrum: "isAjax:true",
  Referer:
    "https://www.marshalls.com/us/store/shop/womens-clothing/_/N-3255077828",
};

function normalizeColorName(colorName = "") {
  if (!colorName) return "NO_COLOR";
  return colorName
    .toUpperCase()
    .trim()
    .replace(/\s+/g, " ")
    .replace(/[^\w\s-]/g, "");
}

function titleCase(str = "") {
  return `${str}`.replace(/\w\S*/g, (txt) => {
    return txt.charAt(0).toUpperCase() + txt.slice(1).toLowerCase();
  });
}

function toFloat(value) {
  const num = parseFloat(value);
  return Number.isFinite(num) ? Number(num.toFixed(2)) : 0;
}

function toAbsoluteUrl(src = "") {
  if (!src) return "";
  if (src.startsWith("//")) return `https:${src}`;
  if (src.startsWith("/")) return `https://www.marshalls.com${src}`;
  return src;
}

function extractPrice(text = "") {
  if (!text) return 0;
  const match = `${text}`.replace(/,/g, "").match(/(\d+(\.\d+)?)/);
  if (!match) return 0;
  return toFloat(match[1]);
}

function cleanText(text = "") {
  return `${text}`.replace(/\s+/g, " ").trim();
}

function delay(ms = 0) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function buildProductUrl(docUrl, listingUrl) {
  if (docUrl && docUrl.startsWith("http")) {
    try {
      const urlObj = new URL(docUrl);
      if (!urlObj.pathname.startsWith("/us/")) {
        const normalizedPath = urlObj.pathname.startsWith("/")
          ? urlObj.pathname
          : `/${urlObj.pathname}`;
        urlObj.pathname = `/us/store${normalizedPath}`;
      }
      return urlObj.toString();
    } catch (error) {
      return docUrl;
    }
  }

  const base = "https://www.marshalls.com";

  if (docUrl) {
    let normalized = docUrl.startsWith("/") ? docUrl : `/${docUrl}`;
    if (!normalized.startsWith("/us/store")) {
      normalized = `/us/store${normalized}`;
    }
    return `${base}${normalized}`;
  }

  if (listingUrl) {
    return listingUrl;
  }

  return "";
}

function deriveGenderFromContext(productUrl = "", category = "") {
  const url = productUrl.toLowerCase();
  const cat = category.toLowerCase();

  if (url.includes("/women") || cat.includes("women")) return "Female";
  if (url.includes("/men") || cat.includes("men")) return "Male";
  if (cat.includes("girls")) return "Girl";
  if (cat.includes("boys")) return "Boy";

  return "";
}

function deriveMaterialsFromBullets(bullets = []) {
  if (!Array.isArray(bullets)) return "";
  const materialBullet = bullets.find((bullet) =>
    /(cotton|polyester|nylon|spandex|wool|silk|leather|rayon|acrylic|linen)/i.test(
      bullet
    )
  );
  return materialBullet || "";
}

function parseProductListing(html) {
  const $ = cheerio.load(html);
  const products = [];

  $(".product").each((index, element) => {
    const $product = $(element);
    const productId = $product.attr("id")?.replace("style-", "") || "";
    if (!productId) return;

    const $link = $product.find(".product-link").first();
    const productUrl = toAbsoluteUrl($link.attr("href") || "");

    const title = $product.find(".product-title").text().trim();
    const brand = $product.find(".product-brand").text().trim();
    const price = $product.find(".product-price").text().trim();
    const comparePrice = $product.find(".price-comparison").text().trim();

    const $img = $product.find(".product-image img").first();
    const imageUrl = toAbsoluteUrl($img.attr("src") || "");
    const alternateImageUrl = toAbsoluteUrl(
      $img.attr("data-altimageurl") || ""
    );

    const colorId = productUrl.match(/colorId=([^&]+)/)?.[1] || "";
    const colorSwatches = [];
    $product.find(".color-options-list .option-link").each((i, el) => {
      colorSwatches.push({
        colorId: $(el).attr("data-colorid") || "",
        colorName:
          $(el).attr("title") || $(el).find(".option-name").text().trim(),
        imageUrl: toAbsoluteUrl($(el).attr("data-imageurl") || ""),
      });
    });

    products.push({
      productId,
      title,
      brand,
      price,
      comparePrice,
      productUrl,
      imageUrl,
      alternateImageUrl,
      alternateImageUrls: alternateImageUrl ? [alternateImageUrl] : [],
      colorId,
      colorName: "",
      colorSwatches,
      department: "",
    });
  });

  return products;
}

async function fetchListingHtml(url, pageOffset = 0) {
  const urlObj = new URL(url);
  if (pageOffset > 0) {
    urlObj.searchParams.set("No", pageOffset.toString());
  }
  const finalUrl = urlObj.toString();

  console.log(`📥 Fetching listing (offset: ${pageOffset}): ${finalUrl}`);
  const response = await fetch(finalUrl, {
    headers: HEADERS,
  });

  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }

  const html = await response.text();
  console.log(`✅ Response length: ${html.length} characters`);
  return html;
}

async function fetchAllListings(baseUrl, targetProductCount = 3000) {
  const productsPerPage = 180; // Marshalls shows 180 products per page
  let allListings = [];
  let currentOffset = 0;
  let currentPage = 1;

  console.log(`\n🔄 Fetching up to ${targetProductCount} products...`);

  while (allListings.length < targetProductCount) {
    try {
      console.log(`\n📄 Page ${currentPage} (offset: ${currentOffset}):`);
      const html = await fetchListingHtml(baseUrl, currentOffset);
      const listings = parseProductListing(html);

      if (listings.length === 0) {
        console.log(
          `   ⚠️ No products found on page ${currentPage}, stopping pagination.`
        );
        break;
      }

      console.log(`   ✅ Found ${listings.length} products on this page`);
      allListings = allListings.concat(listings);
      console.log(`   📦 Total so far: ${allListings.length} products`);

      // Check if we have enough products or reached the last page
      if (allListings.length >= targetProductCount) {
        console.log(`   🏁 Reached target product count`);
        break;
      }

      // Stop if we got significantly fewer products than expected (likely last page)
      if (listings.length < productsPerPage * 0.5) {
        console.log(
          `   🏁 Last page detected (only ${listings.length} products)`
        );
        break;
      }

      currentOffset += productsPerPage;
      currentPage++;

      // Small delay between pages to be respectful
      await delay(WAIT_BETWEEN_REQUESTS);
    } catch (error) {
      console.error(
        `   ❌ Error fetching page ${currentPage}: ${error.message}`
      );
      break;
    }
  }

  // Deduplicate by productId
  const uniqueListings = [];
  const seen = new Set();
  for (const listing of allListings) {
    if (!listing?.productId) continue;
    if (seen.has(listing.productId)) continue;
    seen.add(listing.productId);
    uniqueListings.push(listing);
  }

  console.log(
    `\n✅ Pagination complete: ${allListings.length} total, ${uniqueListings.length} unique products`
  );
  return uniqueListings.slice(0, targetProductCount);
}

async function fetchQuickviewApi(listing) {
  const productId = listing.productId;
  const colorId = listing.colorId || listing.colorSwatches?.[0]?.colorId || "";
  const pos = "1:1";
  const timestamp = Date.now();

  const url = `https://www.marshalls.com/us/store/modal/quickview.jsp?productId=${productId}&colorId=${colorId}&pos=${encodeURIComponent(
    pos
  )}&_=${timestamp}`;

  console.log(`   📡 API URL: ${url}`);

  const response = await fetch(url, {
    headers: HEADERS,
  });

  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }

  const html = await response.text();
  console.log(`   📄 Response length: ${html.length} chars`);

  // Save first product HTML for debugging
  if (productId === "4000398717") {
    fs.writeFileSync(`debug-quickview-${productId}.html`, html);
    console.log(`   💾 Saved quickview HTML for debugging`);
  }

  return parseQuickviewHtml(html, listing);
}

function parseQuickviewHtml(html, listing) {
  const $ = cheerio.load(html);

  // Extract basic info
  const brand = cleanText($(".product-brand").text());
  const title = cleanText($(".product-title").text());
  const priceText = cleanText($(".product-price").text());
  const compareText = cleanText($(".price-comparison").text());

  // Extract description bullets
  const descriptionBullets = [];
  $(".product-description li").each((_, el) => {
    const text = cleanText($(el).text());
    if (text && !text.startsWith("style #:")) {
      descriptionBullets.push(text);
    }
  });

  // Extract image base URL
  const mainImage = $(".main-image");
  let imageBaseUrl = mainImage.attr("data-baseurl") || "";

  if (!imageBaseUrl) {
    const dataSrc = mainImage.attr("data-src") || mainImage.attr("src") || "";
    const match = dataSrc.match(/prd\[([^\]]+)\]/);
    if (match) {
      imageBaseUrl = match[1];
    }
  }

  // Extract colors and sizes from TJXdata.productData JSON
  let colors = [];
  let sizes = [];
  const images = [];
  let variantStockMap = new Map(); // Track stock by color-size combination

  // Find the line with TJXdata.productData and the next line (JSON)
  const lines = html.split("\n");
  const tjxLineIdx = lines.findIndex((line) =>
    line.includes("TJXdata.productData")
  );

  if (tjxLineIdx >= 0 && tjxLineIdx + 1 < lines.length) {
    // JSON is on the next line
    const jsonLine = lines[tjxLineIdx + 1].trim();
    if (jsonLine.startsWith("{")) {
      try {
        // Use eval to parse the JavaScript object (since it has HTML entities)
        const productData = eval("(" + jsonLine + ")");
        const productKey = Object.keys(productData)[0];
        const product = productData[productKey];

        if (product && product.skus) {
          console.log(`   🔍 Found ${product.skus.length} SKUs`);

          const colorMap = new Map();
          const sizeMap = new Map();

          product.skus.forEach((sku) => {
            if (sku.variants) {
              const quantity = parseInt(sku.skuQuantity) || 0;

              // Color variant (NS1058884)
              const colorVariant = sku.variants["NS1058884"];
              const colorId = colorVariant?.id || "";
              const colorName =
                colorVariant?.displayName || colorVariant?.name || "";

              if (colorId) {
                colorMap.set(colorId, {
                  id: colorId,
                  name: colorName,
                });
              }

              // Size variant (NS1058883)
              const sizeVariant = sku.variants["NS1058883"];
              const sizeId = sizeVariant?.id || "";
              const sizeName =
                sizeVariant?.displayName || sizeVariant?.name || "";

              if (sizeId) {
                if (!sizeMap.has(sizeId)) {
                  sizeMap.set(sizeId, {
                    id: sizeId,
                    name: sizeName,
                  });
                }

                // Store stock for this specific color-size combination
                const variantKey = `${colorId}-${sizeId}`;
                variantStockMap.set(variantKey, quantity);
              }
            }
          });

          colors = Array.from(colorMap.values());
          sizes = Array.from(sizeMap.values()).map((size) => ({
            ...size,
            stockByColor: {}, // Will be populated in buildFormattedProduct
          }));

          console.log(
            `   🎨 Extracted ${colors.length} colors: ${colors
              .map((c) => c.name)
              .join(", ")}`
          );
          console.log(
            `   📏 Extracted ${sizes.length} sizes: ${sizes
              .map((s) => s.name)
              .join(", ")}`
          );
          console.log(
            `   📦 Stock info: ${variantStockMap.size} color-size combinations`
          );
        }
      } catch (error) {
        console.warn(`   ⚠️ Failed to parse JSON: ${error.message}`);
      }
    } else {
      console.warn(`   ⚠️ Could not extract TJXdata from line`);
    }
  } else {
    console.warn(`   ⚠️ TJXdata.productData not found in HTML`);
  }

  // Generate high-res image URLs
  if (imageBaseUrl) {
    images.push({
      src: toAbsoluteUrl(
        `//img.marshalls.com/marshalls?set=prd[${imageBaseUrl}],finalSize[2000]&call=url[file:tjxScale.chain]`
      ),
      baseUrl: imageBaseUrl,
    });
  }

  // Add alternate images
  $(".alt-list .thumbnail-image").each((_, img) => {
    const baseUrl = $(img).attr("data-baseurl") || "";
    if (baseUrl && baseUrl !== imageBaseUrl) {
      images.push({
        src: toAbsoluteUrl(
          `//img.marshalls.com/marshalls?set=prd[${baseUrl}],finalSize[2000]&call=url[file:tjxScale.chain]`
        ),
        baseUrl,
      });
    }
  });

  return {
    pid: listing?.productId || "",
    url: buildProductUrl(null, listing?.productUrl),
    brand,
    title,
    priceText,
    compareAtText: compareText.replace(/Compare At\s*/i, "").trim(),
    price: extractPrice(priceText),
    compareAt: extractPrice(compareText),
    colors,
    sizes,
    images,
    descriptionBullets,
    variantStockMap, // Pass stock information for color-size combinations
  };
}

function buildFormattedProduct(detail, listing) {
  if (!detail) return null;

  const productUrl = detail.url || buildProductUrl(null, listing?.productUrl);
  const pid = detail.pid || listing?.productId || "";
  if (!pid) return null;

  const name = detail.title || listing?.title || "";
  const brand = detail.brand || listing?.brand || "";
  const category = "Clothing"; // From URL context

  const descriptionBullets = Array.isArray(detail.descriptionBullets)
    ? detail.descriptionBullets.map((bullet) => bullet.trim()).filter(Boolean)
    : [];

  const description =
    descriptionBullets.length > 0
      ? descriptionBullets.join(" • ")
      : listing?.title || "";

  const gender = deriveGenderFromContext(productUrl, category);

  const sellingPrice = extractPrice(detail.priceText || listing?.price);
  const originalPrice =
    extractPrice(detail.compareAtText || listing?.comparePrice) || sellingPrice;

  const isOnSale =
    originalPrice > 0 && sellingPrice > 0 && sellingPrice < originalPrice;
  const discount = isOnSale
    ? Math.round(((originalPrice - sellingPrice) / originalPrice) * 100)
    : 0;

  // Use only high-res images from detail
  const imageUrls = detail.images.map((img) => img.src).filter(Boolean);

  const colorOptions =
    Array.isArray(detail.colors) && detail.colors.length > 0
      ? detail.colors
      : [{ id: "", name: "", imageUrl: "" }];

  const sizeOptions =
    Array.isArray(detail.sizes) && detail.sizes.length > 0
      ? detail.sizes
      : [{ id: "", name: "", isDisabled: false }];

  const variants = [];
  const variantStockMap = detail.variantStockMap || new Map();

  colorOptions.forEach((colorOption) => {
    const colorId =
      colorOption?.id || colorOption?.value || colorOption?.name || "";
    const colorName =
      colorOption?.name || colorOption?.label || colorOption?.value || "";
    const normalizedColor = normalizeColorName(colorId || colorName);

    // Generate color-specific images
    const colorImageUrls = [];
    if (colorId) {
      // Main image with this color ID
      colorImageUrls.push(
        toAbsoluteUrl(
          `//img.marshalls.com/marshalls?set=prd[${pid}_${colorId}],finalSize[2000]&call=url[file:tjxScale.chain]`
        )
      );
      // Alternate images
      for (let i = 1; i <= 2; i++) {
        colorImageUrls.push(
          toAbsoluteUrl(
            `//img.marshalls.com/marshalls?set=prd[${pid}_alt${i}],finalSize[2000]&call=url[file:tjxScale.chain]`
          )
        );
      }
    } else {
      // Fallback to detail images if no color ID
      colorImageUrls.push(...imageUrls);
    }

    sizeOptions.forEach((sizeOption) => {
      const sizeId = sizeOption?.id || "";
      const sizeValue =
        sizeOption?.name || sizeOption?.label || sizeOption?.value || "";
      const sizeLabel = sizeValue ? `${sizeValue}`.toUpperCase() : "";

      // Check stock for this specific color-size combination
      const variantKey = `${colorId}-${sizeId}`;
      const stockQuantity = variantStockMap.get(variantKey) || 0;
      const isInStock = stockQuantity > 0;

      variants.push({
        price_currency: "USD",
        original_price: originalPrice,
        link_url: productUrl,
        deeplink_url: productUrl,
        image_url: colorImageUrls[0] || "",
        alternate_image_urls: colorImageUrls,
        is_on_sale: isOnSale,
        is_in_stock: isInStock,
        size: sizeLabel,
        color: colorName ? titleCase(colorName) : "",
        mpn: `${pid}-${normalizedColor}`,
        ratings_count: 0,
        average_ratings: 0,
        review_count: 0,
        selling_price: sellingPrice,
        sale_price: isOnSale ? sellingPrice : 0,
        final_price: sellingPrice,
        discount,
        operation_type: "INSERT",
        variant_id: uuidv5(
          `${pid}-${normalizedColor}-${sizeLabel || "DEFAULT"}`,
          VARIANT_NAMESPACE
        ),
        variant_description: "",
      });
    });
  });

  const materials = deriveMaterialsFromBullets(descriptionBullets) || "";

  return {
    parent_product_id: pid,
    name,
    description,
    category,
    retailer_domain: "marshalls.com",
    brand,
    gender,
    materials,
    return_policy_link: STORE_DATA.return_policy_link,
    return_policy: STORE_DATA.return_policy,
    size_chart: "",
    available_bank_offers: "",
    available_coupons: "",
    variants,
    operation_type: "INSERT",
    source: "marshalls",
  };
}

async function writeCatalogFiles(products, storeData = STORE_DATA) {
  const dirPath = path.join(
    __dirname,
    "output",
    storeData.country || "US",
    `marshalls-${storeData.country || "US"}`
  );
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }

  const categories = Array.from(
    new Set(
      products
        .map((product) => product.category)
        .filter((category) => category && category.trim() !== "")
    )
  );

  const storeInfo = {
    name: storeData.name,
    domain: storeData.domain,
    currency: storeData.currency,
    country: storeData.country,
    total_products: products.length,
    categories: categories.length > 0 ? categories : ["Apparel"],
    crawled_at: new Date().toISOString(),
  };

  const payload = {
    store_info: storeInfo,
    products,
  };

  const jsonPath = path.join(dirPath, "catalog.json");
  fs.writeFileSync(jsonPath, JSON.stringify(payload, null, 2), "utf8");

  const jsonlPath = path.join(dirPath, "catalog.jsonl");
  const jsonlContent = products
    .map((product) => JSON.stringify(product))
    .join("\n");
  fs.writeFileSync(jsonlPath, jsonlContent, "utf8");

  return { jsonPath, jsonlPath };
}

async function main() {
  try {
    const TARGET_PER_CATEGORY = 3000; // 3,000 products per category (6,000 total)

    // Fetch listings from both categories with pagination
    console.log("\n🚺 === WOMEN'S CLOTHING ===");
    const womenListings = await fetchAllListings(
      PRODUCT_URLS[0],
      TARGET_PER_CATEGORY
    );
    console.log(`\n✅ Women's total: ${womenListings.length} unique products`);

    console.log("\n\n🚹 === MEN'S CLOTHING ===");
    const menListings = await fetchAllListings(
      PRODUCT_URLS[1],
      TARGET_PER_CATEGORY
    );
    console.log(`\n✅ Men's total: ${menListings.length} unique products`);

    // Combine listings
    const allListings = [...womenListings, ...menListings];
    console.log(
      `\n\n📦 TOTAL PRODUCTS FROM BOTH CATEGORIES: ${allListings.length}`
    );

    // Use all products (no slicing for testing)
    const targetListings = allListings;

    console.log(
      `\n🚚 Fetching product details via Quickview API for ${targetListings.length} listing(s)...`
    );

    const products = [];

    for (let i = 0; i < targetListings.length; i++) {
      const listing = targetListings[i];
      console.log(
        `\n🔍 [${i + 1}/${targetListings.length}] ${listing.productId}: ${
          listing.title || "Unnamed"
        }`
      );

      try {
        const detail = await fetchQuickviewApi(listing);

        console.log(`   ✅ Brand: ${detail.brand}`);
        console.log(`   ✅ Colors: ${detail.colors.length}`);
        console.log(`   ✅ Sizes: ${detail.sizes.length}`);
        console.log(`   ✅ Images: ${detail.images.length} (high-res)`);

        const product = buildFormattedProduct(detail, listing);

        if (product?.variants?.length > 0) {
          products.push(product);
          console.log(`   ✅ Captured ${product.variants.length} variant(s)`);
        } else {
          console.log(`   ⚠️ No variants extracted`);
        }
      } catch (error) {
        console.error(`   ❌ Failed: ${error.message}`);
      }

      // Small delay between requests
      if (i < targetListings.length - 1) {
        await delay(WAIT_BETWEEN_REQUESTS);
      }
    }

    if (products.length === 0) {
      console.warn("\n⚠️ No products were successfully scraped.");
      return;
    }

    console.log(`\n🧾 Formatted products ready for export: ${products.length}`);

    const files = await writeCatalogFiles(products, STORE_DATA);
    console.log("💾 Wrote catalog to:");
    console.log(`   • ${files.jsonPath}`);
    console.log(`   • ${files.jsonlPath}`);
  } catch (error) {
    console.error("❌ Error:", error.message);
  }
}

main();
