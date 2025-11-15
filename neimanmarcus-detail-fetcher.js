/** @format */

/**
 * Neiman Marcus Product Detail Fetcher
 *
 * Reads product listing JSON and fetches full product details for a specific chunk
 * You can process chunks one at a time by entering the chunk number
 *
 * USAGE:
 * 1. Open browser console on Neiman Marcus website
 * 2. Load your product listing JSON file (see instructions below)
 * 3. Paste this entire script
 * 4. When prompted, enter the chunk number you want to process:
 *    - Enter 1: processes products 1-100 → downloads chunk-1.json
 *    - Enter 2: processes products 101-200 → downloads chunk-2.json
 *    - Enter 3: processes products 201-300 → downloads chunk-3.json
 *    - etc.
 *
 * TO LOAD JSON FILE:
 * Option 1: Use file input (recommended for large files)
 *   - The script will create a file input when you run it
 *   - Select your neimanmarcus-product-listings-*.json file
 *
 * Option 2: Paste JSON directly
 *   - Copy the JSON content from your file
 *   - Set: window.productListingsData = <paste your JSON here>
 *   - Then run the script
 */

(async () => {
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

  const CHUNK_SIZE = 100;
  const DELAY_BETWEEN_PRODUCTS = 500; // 500ms delay between products
  const DELAY_BETWEEN_CHUNKS = 5000; // 5 seconds delay between chunks

  /**
   * Generate UUID v5 hash (simple implementation for browser)
   */
  function generateUUIDv5(name, namespace) {
    const str = namespace + name;
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      const char = str.charCodeAt(i);
      hash = (hash << 5) - hash + char;
      hash = hash & hash; // Convert to 32bit integer
    }
    const hex = Math.abs(hash).toString(16).padStart(8, "0");
    return `${hex.slice(0, 8)}-${hex.slice(0, 4)}-5${hex.slice(
      1,
      4
    )}-${hex.slice(0, 4)}-${hex.slice(0, 12)}`;
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
          return productData;
        }
      }
    } catch (e) {
      console.log(`  ⚠️ Error extracting __NEXT_DATA__: ${e.message}`);
    }
    return null;
  }

  /**
   * Extract comprehensive product details from productData object
   */
  function extractFromProductData(productData) {
    if (!productData) return null;

    const parentId = productData.id || "";
    const msid =
      productData.metadata?.pimStyle || productData.metadata?.masterStyle || "";
    const name = productData.name || "";
    const description = productData.details?.longDesc || "";
    const brand = productData.designer?.name || "Neiman Marcus";

    const gender =
      productData.genderCode === "Women"
        ? "Female"
        : productData.genderCode === "Men"
        ? "Male"
        : "";

    let category = "";
    if (productData.hierarchy && productData.hierarchy.length > 0) {
      category = productData.hierarchy[0].level1 || "";
    }

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

    const fitTypes =
      productData.attributesMap?.fitDetails || productData.fitDetails || "";

    const originalPrice = parseFloat(productData.price?.retailPrice || 0);
    const salePrice = parseFloat(
      productData.price?.salePrice || productData.price?.promoPrice || 0
    );
    const finalPrice =
      salePrice > 0 && salePrice < originalPrice ? salePrice : originalPrice;

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

          if (colorVal.media?.main?.dynamic?.url) {
            images.push(colorVal.media.main.dynamic.url);
          }

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

        let variantImageUrl = "";
        let variantAlternateImages = [];

        if (colorImages[color] && colorImages[color].length > 0) {
          variantImageUrl = colorImages[color][0];
          variantAlternateImages = colorImages[color].slice(0);
        } else if (productImages.length > 0) {
          variantImageUrl = productImages[0];
          variantAlternateImages = productImages.slice(0);
        }

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

        const isInStock = sku.inStock || sku.sellable || false;
        const stockLevel = sku.stockLevel || 0;

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
            ? Math.round(
                ((variantPrice - variantSalePrice) / variantPrice) * 100
              )
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
      materials: materials || "",
      return_policy_link:
        "https://www.neimanmarcus.com/assistance/assistance.jsp#returns",
      return_policy:
        "Neiman Marcus offers returns within 30 days of purchase with original receipt.",
      size_chart: "",
      available_bank_offers: "",
      available_coupons: "",
      variants: variants,
      operation_type: "INSERT",
      source: "neimanmarcus",
    };
  }

  /**
   * Fetch product details from product page HTML
   */
  async function fetchProductDetails(product) {
    const msid = product.msid || product.id;
    const productId = product.id; // prod285090044 format
    const canonical = product.canonical || product.url || product.productUrl;
    const url = canonical?.startsWith("http")
      ? canonical
      : `https://www.neimanmarcus.com${canonical}`;

    try {
      const htmlRes = await fetch(url, {
        headers: HEADERS,
        mode: "cors",
        credentials: "include",
      });

      if (htmlRes.ok) {
        const html = await htmlRes.text();
        const productData = extractProductDataFromNextData(html);

        if (productData) {
          const formattedProduct = extractFromProductData(productData);

          if (
            formattedProduct &&
            formattedProduct.variants &&
            formattedProduct.variants.length > 0
          ) {
            return formattedProduct;
          }
        }
      }
    } catch (e) {
      console.log(`  ❌ HTML fetch failed: ${e.message}`);
    }

    return null;
  }

  /**
   * Download JSON file
   */
  function downloadJSON(data, filename) {
    const jsonString = JSON.stringify(data, null, 2);
    const blob = new Blob([jsonString], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  }

  /**
   * Load JSON file
   */
  function loadJSONFile() {
    return new Promise((resolve, reject) => {
      const input = document.createElement("input");
      input.type = "file";
      input.accept = "application/json";
      input.onchange = (e) => {
        const file = e.target.files[0];
        if (!file) {
          reject(new Error("No file selected"));
          return;
        }

        const reader = new FileReader();
        reader.onload = (event) => {
          try {
            const data = JSON.parse(event.target.result);
            resolve(data);
          } catch (error) {
            reject(new Error(`Failed to parse JSON: ${error.message}`));
          }
        };
        reader.onerror = () => reject(new Error("Failed to read file"));
        reader.readAsText(file);
      };
      input.click();
    });
  }

  // Main execution
  console.log("🚀 Starting Neiman Marcus Product Detail Fetcher...");
  console.log(
    `📌 You will be prompted to select which chunk to process (${CHUNK_SIZE} products per chunk)\n`
  );

  let productListingsData = null;

  // Check if data is already loaded
  if (window.productListingsData) {
    console.log("✅ Using pre-loaded product listings data");
    productListingsData = window.productListingsData;
  } else {
    // Try to load from file
    console.log("📁 Please select your product listings JSON file...");
    try {
      productListingsData = await loadJSONFile();
      console.log("✅ JSON file loaded successfully");
    } catch (error) {
      console.error(
        "❌ Error loading file:",
        error.message,
        "\n\n💡 Alternative: Set window.productListingsData = <your JSON data> and run again"
      );
      return;
    }
  }

  // Extract products array
  let products = [];
  if (
    productListingsData.products &&
    Array.isArray(productListingsData.products)
  ) {
    products = productListingsData.products;
  } else if (Array.isArray(productListingsData)) {
    products = productListingsData;
  } else {
    console.error(
      "❌ Invalid JSON structure. Expected 'products' array or array of products"
    );
    return;
  }

  const totalProducts = products.length;
  const totalChunks = Math.ceil(totalProducts / CHUNK_SIZE);

  console.log(`\n📊 Total products available: ${totalProducts}`);
  console.log(
    `📦 Total chunks available: ${totalChunks} (${CHUNK_SIZE} products per chunk)\n`
  );

  // Prompt user for chunk number
  const chunkNumberInput = prompt(
    `Which chunk do you want to process?\n\n` +
      `Enter a number between 1 and ${totalChunks}:\n` +
      `  - Chunk 1: products 1-${CHUNK_SIZE}\n` +
      `  - Chunk 2: products ${CHUNK_SIZE + 1}-${CHUNK_SIZE * 2}\n` +
      `  - Chunk ${totalChunks}: products ${
        (totalChunks - 1) * CHUNK_SIZE + 1
      }-${totalProducts}\n\n` +
      `Enter chunk number:`
  );

  if (!chunkNumberInput) {
    console.log("❌ No chunk number provided. Exiting.");
    return;
  }

  const chunkNumber = parseInt(chunkNumberInput, 10);

  if (isNaN(chunkNumber) || chunkNumber < 1 || chunkNumber > totalChunks) {
    console.error(
      `❌ Invalid chunk number. Please enter a number between 1 and ${totalChunks}.`
    );
    return;
  }

  // Calculate indices for the selected chunk (chunkNumber is 1-based)
  const chunkIndex = chunkNumber - 1; // Convert to 0-based index
  const startIndex = chunkIndex * CHUNK_SIZE;
  const endIndex = Math.min(startIndex + CHUNK_SIZE, totalProducts);
  const chunkProducts = products.slice(startIndex, endIndex);

  console.log(
    `\n🔄 Processing chunk ${chunkNumber} (products ${
      startIndex + 1
    }-${endIndex})`
  );
  console.log(`🔍 Fetching details for ${chunkProducts.length} products...\n`);

  const results = [];

  for (let i = 0; i < chunkProducts.length; i++) {
    const p = chunkProducts[i];
    try {
      const formattedProduct = await fetchProductDetails(p);

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
      await new Promise((r) => setTimeout(r, DELAY_BETWEEN_PRODUCTS));
    } catch (err) {
      console.log(
        `❌ [${i + 1}/${chunkProducts.length}] ${p.id || p.msid} failed:`,
        err.message
      );
    }
  }

  // Download chunk results
  if (results.length > 0) {
    const filename = `chunk-${chunkNumber}.json`;

    downloadJSON(results, filename);
    console.log(`\n💾 Chunk ${chunkNumber} JSON file downloaded: ${filename}`);
    console.log(`📊 Chunk ${chunkNumber} contains ${results.length} products`);
    console.log(
      `📦 Chunk ${chunkNumber} total variants: ${results.reduce(
        (sum, p) => sum + (p.variants?.length || 0),
        0
      )}`
    );
  } else {
    console.log(`\n⚠️ Chunk ${chunkNumber} has no results to download`);
  }

  console.log(`\n✅ Chunk ${chunkNumber} processing complete!`);
})();
