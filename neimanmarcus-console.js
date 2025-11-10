/** @format */

/**
 * Neiman Marcus Console Crawler with Comprehensive Data Extraction
 *
 * MASTER DATA SOURCE: __NEXT_DATA__ (productData object in HTML)
 * This contains ALL product details: description, materials, SKUs, colors, sizes, images, availability, stock levels
 *
 * The script prioritizes extracting from __NEXT_DATA__ which has complete product information.
 * This is embedded in every product page's HTML and contains everything needed.
 *
 * USAGE:
 * 1. Open browser console on a Neiman Marcus product page
 * 2. Paste this entire script
 * 3. The script will automatically extract all product data from __NEXT_DATA__
 *
 * For API discovery/testing:
 * - Run: await discoverAPIEndpoints(window.location.href)
 * - Run: await testProductAPIs("prod285090044", "285090044", "/p/product-name")
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

  /**
   * Extract and generate all image variants from a base image URL
   * Neiman Marcus uses suffixes: _m (main), _a, _b, _c, _z
   * Format: https://media.neimanmarcus.com/f_auto,q_auto:low,ar_4:5,c_fill,dpr_2.0,w_790/01/nm_{msid}_{colorCode}_{suffix}
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
   * Generate UUID v5 hash (simple implementation for browser)
   */
  function generateUUIDv5(name, namespace) {
    // Simple hash function for browser compatibility
    const str = namespace + name;
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      const char = str.charCodeAt(i);
      hash = (hash << 5) - hash + char;
      hash = hash & hash; // Convert to 32bit integer
    }
    // Convert to UUID-like format
    const hex = Math.abs(hash).toString(16).padStart(8, "0");
    return `${hex.slice(0, 8)}-${hex.slice(0, 4)}-5${hex.slice(
      1,
      4
    )}-${hex.slice(0, 4)}-${hex.slice(0, 12)}`;
  }

  /**
   * Discover API endpoints by intercepting fetch requests on a product page
   * Run this in browser console on a product page to find the real API
   */
  async function discoverAPIEndpoints(productUrl) {
    console.log(`🔍 Discovering API endpoints for: ${productUrl}`);

    const interceptedRequests = [];
    const interceptedResponses = [];
    const productAPIs = [];

    // Helper to check if response contains comprehensive product data
    function hasComprehensiveProductData(data) {
      if (!data) return false;
      const str = JSON.stringify(data).toLowerCase();
      const hasBasic =
        str.includes("msid") || str.includes("productid") || str.includes("id");
      const hasDetails =
        str.includes("description") ||
        str.includes("materials") ||
        str.includes("composition");
      const hasVariants =
        str.includes("variants") ||
        str.includes("skus") ||
        (str.includes("sizes") && str.includes("colors"));
      const hasImages =
        str.includes("images") ||
        str.includes("mediaset") ||
        str.includes("imageurl");
      return hasBasic && (hasDetails || hasVariants || hasImages);
    }

    // Override fetch to intercept requests
    const originalFetch = window.fetch;
    window.fetch = async function (...args) {
      const url = args[0];
      const options = args[1] || {};
      const urlString =
        typeof url === "string" ? url : url.url || url.toString();

      // Log API-like requests
      if (
        urlString.includes("/api/") ||
        urlString.includes("/graphql") ||
        urlString.includes("/pdp/") ||
        urlString.includes("/dt/api/") ||
        urlString.includes("/c/dt/api/") ||
        urlString.includes("product") ||
        urlString.includes("prod-beige") ||
        urlString.includes("composite") ||
        urlString.includes("details") ||
        urlString.endsWith(".json")
      ) {
        interceptedRequests.push({
          url: urlString,
          method: options.method || "GET",
          headers: options.headers,
          body: options.body,
        });
        console.log(
          `📤 Intercepted: [${options.method || "GET"}] ${urlString}`
        );
      }

      const response = await originalFetch.apply(this, args);

      // Clone response to read it without consuming it
      const clonedResponse = response.clone();

      // Try to parse JSON responses
      if (
        urlString.includes("/api/") ||
        urlString.includes("/graphql") ||
        urlString.includes("/pdp/") ||
        urlString.includes("/dt/api/") ||
        urlString.includes("/c/dt/api/") ||
        urlString.includes("product") ||
        urlString.includes("composite") ||
        urlString.includes("details")
      ) {
        try {
          const contentType = clonedResponse.headers.get("content-type");
          if (contentType && contentType.includes("application/json")) {
            const data = await clonedResponse.json();
            interceptedResponses.push({
              url: urlString,
              status: response.status,
              data: data,
            });

            // Check if this is comprehensive product data
            if (hasComprehensiveProductData(data)) {
              const completeness = {
                hasDescription: JSON.stringify(data)
                  .toLowerCase()
                  .includes("description"),
                hasMaterials:
                  JSON.stringify(data).toLowerCase().includes("materials") ||
                  JSON.stringify(data).toLowerCase().includes("composition"),
                hasVariants:
                  JSON.stringify(data).toLowerCase().includes("variants") ||
                  JSON.stringify(data).toLowerCase().includes("skus"),
                hasSizes: JSON.stringify(data).toLowerCase().includes("sizes"),
                hasColors: JSON.stringify(data)
                  .toLowerCase()
                  .includes("colors"),
                hasImages:
                  JSON.stringify(data).toLowerCase().includes("images") ||
                  JSON.stringify(data).toLowerCase().includes("mediaset"),
              };

              productAPIs.push({
                url: urlString,
                method: options.method || "GET",
                headers: options.headers,
                completeness: completeness,
                data: data,
              });

              console.log(`✅ FOUND COMPREHENSIVE PRODUCT API: ${urlString}`);
              console.log(`📊 Completeness:`, completeness);
              console.log(`📦 Sample data keys:`, Object.keys(data));
            } else if (
              data.msid ||
              data.productId ||
              data.id ||
              (data.data && (data.data.msid || data.data.productId)) ||
              (data.product && (data.product.msid || data.product.id)) ||
              (Array.isArray(data) &&
                data.length > 0 &&
                (data[0].msid || data[0].productId))
            ) {
              console.log(`⚠️ Found basic product API: ${urlString}`);
              console.log(`📦 Response data keys:`, Object.keys(data));
            }
          }
        } catch (e) {
          // Not JSON or can't parse
        }
      }

      return response;
    };

    // Navigate to page (if not already there)
    if (window.location.href !== productUrl) {
      window.location.href = productUrl;
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }

    // Wait for page to load
    await new Promise((resolve) => setTimeout(resolve, 5000));

    console.log(`\n📊 Discovery Summary:`);
    console.log(`   📤 Intercepted ${interceptedRequests.length} requests`);
    console.log(`   📥 Intercepted ${interceptedResponses.length} responses`);
    console.log(`   ✅ Found ${productAPIs.length} comprehensive product APIs`);

    if (productAPIs.length > 0) {
      console.log(`\n🎯 Recommended APIs (sorted by completeness):`);
      productAPIs.forEach((api, idx) => {
        const score = Object.values(api.completeness).filter(Boolean).length;
        console.log(`\n${idx + 1}. ${api.url}`);
        console.log(`   Method: ${api.method}`);
        console.log(`   Completeness Score: ${score}/6`);
        console.log(`   Details:`, api.completeness);
      });
    }

    return {
      requests: interceptedRequests,
      responses: interceptedResponses,
      productAPIs: productAPIs,
    };
  }

  /**
   * Fetch product details from the master GraphQL API
   * This is the comprehensive API endpoint found in the HTML: https://prod-d-web-w2.api-nm.cloud/graphql-pdp-service/products
   */
  async function fetchFromGraphQLAPI(productId, msid) {
    const graphqlUrl =
      "https://prod-d-web-w2.api-nm.cloud/graphql-pdp-service/products";

    // Try different GraphQL query patterns
    const queries = [
      // Pattern 1: Query by productId
      {
        query: `
          query GetProduct($productId: String!) {
            product(productId: $productId) {
              id
              name
              description
              brand
              designer
              price
              salePrice
              images
              variants {
                id
                color
                size
                price
                available
                images
              }
              skus {
                id
                color
                size
                available
                stockLevel
                images
              }
            }
          }
        `,
        variables: { productId: productId || `prod${msid}` },
      },
      // Pattern 2: Query by msid
      {
        query: `
          query GetProductByMsid($msid: String!) {
            product(msid: $msid) {
              id
              name
              description
              brand
              designer
              price
              salePrice
              images
              variants {
                id
                color
                size
                price
                available
                images
              }
              skus {
                id
                color
                size
                available
                stockLevel
                images
              }
            }
          }
        `,
        variables: { msid: msid || productId?.replace("prod", "") },
      },
      // Pattern 3: Simple query
      {
        query: `{ product(id: "${
          productId || `prod${msid}`
        }") { id name description } }`,
      },
    ];

    for (const queryConfig of queries) {
      try {
        console.log(`  🔍 Trying GraphQL query pattern...`);
        const res = await fetch(graphqlUrl, {
          method: "POST",
          headers: {
            ...HEADERS,
            "Content-Type": "application/json",
          },
          body: JSON.stringify(queryConfig),
          mode: "cors",
          credentials: "include",
        });

        if (res.ok) {
          const data = await res.json();
          if (data.data && data.data.product) {
            console.log(`  ✅ GraphQL API success!`);
            return data.data.product;
          }
        }
      } catch (e) {
        console.log(`  ⚠️ GraphQL query failed: ${e.message}`);
      }
    }

    return null;
  }

  /**
   * Extract product data from __NEXT_DATA__ in HTML
   * This contains ALL product details embedded in the page (productData object)
   * This is the MASTER data source - contains everything: description, materials, SKUs, colors, sizes, images, availability
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
   * API provides all fields - we only need to generate alternate_image_urls
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
          // Include main image in alternate images, then add rest
          variantAlternateImages = colorImages[color].slice(0); // Include all including main
        } else if (productImages.length > 0) {
          // Fallback to product-level images
          variantImageUrl = productImages[0];
          variantAlternateImages = productImages.slice(0); // Include all including main
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
          // If it's not first, move it to first position
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
   * Test multiple potential API endpoints to find the most comprehensive one
   */
  async function testProductAPIs(productId, msid, canonical) {
    console.log(
      `🧪 Testing multiple API endpoints for product: ${productId || msid}`
    );

    const apiTests = [];

    // List of potential API endpoints to test
    const apiEndpoints = [
      // MASTER GraphQL API (found in HTML - most comprehensive)
      {
        name: "GraphQL PDP Service (MASTER API)",
        url: "https://prod-d-web-w2.api-nm.cloud/graphql-pdp-service/products",
        method: "POST",
        isGraphQL: true,
      },
      // Prod-Beige API (found in HTML)
      {
        name: "Prod-Beige API",
        url: `https://prod-beige.api-nm.io/api/products/${productId || msid}`,
        method: "GET",
      },
      // Current composite API
      {
        name: "Composite Minified Product Details",
        url: `https://www.neimanmarcus.com/dt/api/composite/minifiedproductdetails?productIds=${productId}`,
        method: "GET",
      },
      // Try with msid instead of productId
      {
        name: "Composite Minified Product Details (MSID)",
        url: `https://www.neimanmarcus.com/dt/api/composite/minifiedproductdetails?productIds=prod${msid}`,
        method: "GET",
      },
      // Try full product details API
      {
        name: "Product Details API",
        url: `https://www.neimanmarcus.com/dt/api/productdetails?productId=${productId}`,
        method: "GET",
      },
      {
        name: "Product Details API (MSID)",
        url: `https://www.neimanmarcus.com/dt/api/productdetails?msid=${msid}`,
        method: "GET",
      },
      // Try PDP API
      {
        name: "PDP API",
        url: `https://www.neimanmarcus.com/dt/api/pdp?productId=${productId}`,
        method: "GET",
      },
      // Try product info API
      {
        name: "Product Info API",
        url: `https://www.neimanmarcus.com/dt/api/productinfo?productId=${productId}`,
        method: "GET",
      },
      // Try from canonical URL pattern
      {
        name: "Product API from Canonical",
        url: canonical ? `${canonical}.json` : null,
        method: "GET",
      },
      {
        name: "Product API from Canonical (alt)",
        url: canonical ? `${canonical}?format=json` : null,
        method: "GET",
      },
    ];

    // Test each endpoint
    for (const endpoint of apiEndpoints) {
      if (!endpoint.url) continue;

      try {
        console.log(`  🔍 Testing: ${endpoint.name}`);

        let res;
        if (endpoint.isGraphQL) {
          // GraphQL POST request
          const graphqlQuery = {
            query: `
              query GetProduct($productId: String!) {
                product(productId: $productId) {
                  id
                  name
                  description
                  brand
                  designer
                  price
                  salePrice
                  images
                  variants {
                    id
                    color
                    size
                    price
                    available
                    images
                  }
                  skus {
                    id
                    color
                    size
                    available
                    stockLevel
                    images
                  }
                }
              }
            `,
            variables: { productId: productId || `prod${msid}` },
          };

          res = await fetch(endpoint.url, {
            method: "POST",
            headers: {
              ...HEADERS,
              "Content-Type": "application/json",
            },
            body: JSON.stringify(graphqlQuery),
            mode: "cors",
            credentials: "include",
          });
        } else {
          // Regular GET request
          res = await fetch(endpoint.url, {
            headers: HEADERS,
            mode: "cors",
            credentials: "include",
          });
        }

        if (res.ok) {
          const data = await res.json();

          // Analyze completeness
          const dataStr = JSON.stringify(data).toLowerCase();
          const completeness = {
            hasDescription:
              dataStr.includes("description") ||
              dataStr.includes("longdescription") ||
              dataStr.includes("productdescription"),
            hasMaterials:
              dataStr.includes("materials") ||
              dataStr.includes("composition") ||
              dataStr.includes("fabric") ||
              dataStr.includes("firstmaterial"),
            hasVariants:
              dataStr.includes("variants") || dataStr.includes("skus"),
            hasSizes:
              dataStr.includes("sizes") ||
              dataStr.includes("sizeoptions") ||
              (dataStr.includes("size") && dataStr.includes("name")),
            hasColors:
              dataStr.includes("colors") ||
              dataStr.includes("colorvariants") ||
              (dataStr.includes("color") && dataStr.includes("name")),
            hasImages:
              dataStr.includes("images") ||
              dataStr.includes("mediaset") ||
              dataStr.includes("imageurl") ||
              dataStr.includes("media"),
            hasGender:
              dataStr.includes("gender") ||
              dataStr.includes("sex") ||
              dataStr.includes("classification"),
            hasCategory:
              dataStr.includes("category") ||
              dataStr.includes("hierarchy") ||
              dataStr.includes("breadcrumbs"),
            hasFitTypes:
              dataStr.includes("fit") ||
              dataStr.includes("fittypes") ||
              dataStr.includes("fitdetails"),
            hasAvailability:
              dataStr.includes("available") ||
              dataStr.includes("instock") ||
              dataStr.includes("sellable") ||
              dataStr.includes("stocklevel"),
          };

          const score = Object.values(completeness).filter(Boolean).length;

          apiTests.push({
            name: endpoint.name,
            url: endpoint.url,
            method: endpoint.method,
            status: res.status,
            completeness: completeness,
            score: score,
            data: data,
            dataKeys: Object.keys(data),
          });

          console.log(
            `    ✅ Status: ${res.status}, Completeness Score: ${score}/10`
          );
        } else {
          console.log(`    ❌ Status: ${res.status}`);
        }
      } catch (e) {
        console.log(`    ❌ Error: ${e.message}`);
      }
    }

    // Sort by completeness score
    apiTests.sort((a, b) => b.score - a.score);

    console.log(`\n📊 API Test Results (sorted by completeness):`);
    apiTests.forEach((test, idx) => {
      console.log(`\n${idx + 1}. ${test.name}`);
      console.log(`   URL: ${test.url}`);
      console.log(`   Score: ${test.score}/10`);
      console.log(`   Completeness:`, test.completeness);
      console.log(`   Top-level keys:`, test.dataKeys.slice(0, 10));
    });

    return apiTests;
  }

  /**
   * Fetch product details using the best available API endpoint
   * Tests multiple endpoints and uses the one with most complete data
   */
  async function fetchProductDetailsFromBestAPI(product) {
    const msid = product.msid || product.id;
    const productId = product.id; // prod285090044 format
    const canonical = product.canonical || product.url || product.productUrl;

    console.log(`  🔍 Finding best API for: ${msid} (${productId})`);

    // Test all potential APIs
    const apiTests = await testProductAPIs(productId, msid, canonical);

    if (apiTests.length === 0) {
      console.log(`  ⚠️ No working APIs found, falling back to composite API`);
      return null;
    }

    // Use the API with highest completeness score
    const bestAPI = apiTests[0];
    console.log(
      `  ✅ Using best API: ${bestAPI.name} (Score: ${bestAPI.score}/10)`
    );
    console.log(`  📋 API URL: ${bestAPI.url}`);

    return bestAPI.data;
  }

  /**
   * Fetch product details from __NEXT_DATA__ (productData)
   * API provides all fields - we only generate alternate_image_urls
   */
  async function fetchProductDetails(product) {
    const msid = product.msid || product.id;
    const productId = product.id; // prod285090044 format
    const canonical = product.canonical || product.url || product.productUrl;
    const url = canonical?.startsWith("http")
      ? canonical
      : `https://www.neimanmarcus.com${canonical}`;

    console.log(`  🔍 Fetching: ${msid} (${productId}) - ${url}`);

    try {
      const htmlRes = await fetch(url, {
        headers: HEADERS,
        mode: "cors",
        credentials: "include",
      });

      if (htmlRes.ok) {
        const html = await htmlRes.text();
        console.log(
          `  ✅ HTML fetched (${(html.length / 1024).toFixed(2)} KB)`
        );

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
            console.log(`  📊 Data completeness:`, {
              hasDescription: !!formattedProduct.description,
              hasMaterials: !!formattedProduct.materials,
              hasGender: !!formattedProduct.gender,
              hasFitTypes: !!formattedProduct.fit_types,
              variantsCount: formattedProduct.variants.length,
              avgAlternateImages: Math.round(
                formattedProduct.variants.reduce(
                  (sum, v) => sum + (v.alternate_image_urls?.length || 0),
                  0
                ) / formattedProduct.variants.length
              ),
            });
            return formattedProduct;
          }
        }
      } else {
        console.log(`  ❌ HTML fetch returned status ${htmlRes.status}`);
      }
    } catch (e) {
      console.log(`  ❌ HTML fetch failed: ${e.message}`);
    }

    console.log(`  ❌ Could not extract complete product data for ${msid}`);
    return null;
  }

  /**
   * Fetch all products from listing API - fetches all pages
   */
  async function fetchAllProducts() {
    const all = [];
    let page = 1;
    let hasMore = true;

    while (hasMore) {
      const listUrl = `https://www.neimanmarcus.com/c/dt/api/productlisting?categoryId=cat58290731&page=${page}&parentCategoryId=&navPath=`;

      try {
        const res = await fetch(listUrl, { headers: HEADERS });
        if (!res.ok) {
          console.log(`Page ${page} returned status ${res.status}`);
          hasMore = false;
          break;
        }

        const data = await res.json();
        const products = data?.products || [];

        if (products.length > 0) {
          // Log first product structure for debugging (only on first page)
          if (page === 1 && products[0]) {
            console.log(
              `📋 Sample product from listing API:`,
              Object.keys(products[0])
            );
            console.log(`📋 First product data:`, products[0]);
          }

          console.log(
            `✅ Fetched ${products.length} products from page ${page}`
          );
          all.push(...products);
          page++;

          // Check if there are more pages (you may need to adjust this based on API response)
          // If products array is empty or shorter than expected, assume no more pages
          if (products.length === 0) {
            hasMore = false;
          }

          // Add delay between page requests to avoid rate limits
          await new Promise((r) => setTimeout(r, 1000));
        } else {
          hasMore = false;
        }
      } catch (error) {
        console.log(`❌ Error fetching page ${page}:`, error.message);
        hasMore = false;
      }
    }

    console.log(`\n✅ Total products collected: ${all.length}`);
    return all;
  }

  // Main execution
  console.log("🚀 Starting Neiman Marcus crawler...");
  console.log("📌 Processing all products in chunks of 500\n");

  const allProducts = await fetchAllProducts();
  const CHUNK_SIZE = 100;
  const totalChunks = Math.ceil(allProducts.length / CHUNK_SIZE);

  console.log(`\n📊 Total products to process: ${allProducts.length}`);
  console.log(
    `📦 Will process in ${totalChunks} chunk(s) of ${CHUNK_SIZE} products\n`
  );

  // Download results as JSON file
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

  // Process products in chunks
  for (let chunkIndex = 0; chunkIndex < totalChunks; chunkIndex++) {
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
      const p = chunkProducts[i];
      try {
        // First, try to create product from listing data if it has enough info
        let formattedProduct = null;

        // Check if listing product has enough data (using correct field names from API)
        if (p.name && (p.rprc || p.msid)) {
          console.log(`  📋 Using listing data for ${p.id || p.msid}`);
          const parentId = p.id || p.msid || ""; // Use id (prod285090044) as parent_product_id
          const msid = p.msid || "";

          // Extract price - rprc is regular price (e.g., "$259.99"), need to remove currency symbols
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

          // Extract image - main might be image URL or object
          // Handle protocol-relative URLs (starting with //)
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
              // Generate all image variants from the base URL
              const imageVariants = generateImageVariants(
                baseImageUrl,
                msid,
                null
              );
              if (imageVariants.length > 0) {
                imageUrl = imageVariants[0]; // First one (_m) is the main image
                alternateImages = imageVariants.slice(1); // Rest are alternates
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
              sale_price: salePrice, ////ac
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
            description: "", // Description not in listing API
            category: "Clothing",
            retailer_domain: "neimanmarcus.com",
            brand: p.designer || p.brand || "Neiman Marcus",
            gender: "Women", // Not in listing API
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

          console.log(
            `  ✅ Created product from listing data: ${formattedProduct.name}`
          );

          // Try to enrich with detail API/HTML data (for description, materials, sizes, etc.)
          // This runs in parallel - we'll merge the data
          try {
            const detailProduct = await fetchProductDetails(p);
            if (detailProduct) {
              // Merge detail data into listing product
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
              // Always use detail variants if available (they have complete data from API)
              if (detailProduct.variants && detailProduct.variants.length > 0) {
                formattedProduct.variants = detailProduct.variants;
                console.log(
                  `  ✅ Enriched with ${detailProduct.variants.length} variants from detail API`
                );
              }
            }
          } catch (e) {
            // Silently continue if detail fetch fails
            console.log(`  ⚠️ Could not enrich with detail data: ${e.message}`);
          }
        }

        // If listing data wasn't enough, try fetching details
        if (
          !formattedProduct ||
          !formattedProduct.variants ||
          formattedProduct.variants.length === 0
        ) {
          formattedProduct = await fetchProductDetails(p);
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
          // Log product data for debugging
          console.log(`  📋 Product data:`, {
            id: p.id,
            msid: p.msid,
            name: p.name,
            price: p.price,
            hasCanonical: !!p.canonical,
            hasUrl: !!p.url,
          });
        }

        // Delay between product requests
        await new Promise((r) => setTimeout(r, 500));
      } catch (err) {
        console.log(
          `❌ [${i + 1}/${chunkProducts.length}] ${p.id || p.msid} failed:`,
          err.message
        );
      }
    }

    // Download chunk results
    if (results.length > 0) {
      const timestamp = new Date()
        .toISOString()
        .replace(/[:.]/g, "-")
        .slice(0, -5);
      const filename = `neimanmarcus-catalog-chunk-${
        chunkIndex + 1
      }-of-${totalChunks}-${startIndex + 1}-to-${endIndex}-${timestamp}.json`;

      downloadJSON(results, filename);
      console.log(
        `\n💾 Chunk ${chunkIndex + 1} JSON file downloaded: ${filename}`
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
    } else {
      console.log(`\n⚠️ Chunk ${chunkIndex + 1} has no results to download`);
    }

    // Add delay between chunks to avoid rate limits (except for the last chunk)
    if (chunkIndex < totalChunks - 1) {
      console.log(`\n⏳ Waiting 5 seconds before processing next chunk...`);
      await new Promise((r) => setTimeout(r, 5000));
    }
  }

  console.log("\n✅ All chunks processed!");
  console.log(`📊 Total products processed: ${allProducts.length}`);

  // Export functions for manual testing
  window.testProductAPIs = testProductAPIs;
  window.discoverAPIEndpoints = discoverAPIEndpoints;
  window.fetchProductDetailsFromBestAPI = fetchProductDetailsFromBestAPI;
  window.fetchFromGraphQLAPI = fetchFromGraphQLAPI;
  window.extractProductDataFromNextData = extractProductDataFromNextData;
  window.extractFromProductData = extractFromProductData;

  console.log("\n💡 Helper functions available:");
  console.log(
    "   - testProductAPIs(productId, msid, canonical) - Test all API endpoints"
  );
  console.log(
    "   - discoverAPIEndpoints(productUrl) - Discover APIs by intercepting requests"
  );
  console.log(
    "   - fetchProductDetailsFromBestAPI(product) - Fetch using best API"
  );
  console.log(
    "   - fetchFromGraphQLAPI(productId, msid) - Fetch from GraphQL master API"
  );
  console.log(
    "   - extractProductDataFromNextData(html) - Extract productData from __NEXT_DATA__"
  );
  console.log(
    "   - extractFromProductData(productData) - Format productData into catalog format"
  );
  console.log("\n📝 Example usage:");
  console.log(
    '   await testProductAPIs("prod285090044", "285090044", "/p/product-name");'
  );
  console.log(
    '   await discoverAPIEndpoints("https://www.neimanmarcus.com/p/product-name");'
  );
  console.log('   await fetchFromGraphQLAPI("prod285090044", "285090044");');
  console.log("\n🎯 MASTER DATA SOURCE:");
  console.log(
    "   __NEXT_DATA__ (productData object in HTML) - Contains ALL product details!"
  );
  console.log(
    "   This includes: description, materials, SKUs, colors, sizes, images, availability, stock levels"
  );
  console.log("   Location: window.__NEXT_DATA__.props.pageProps.productData");
})();
