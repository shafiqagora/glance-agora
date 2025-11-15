/** @format */

/**
 * Neiman Marcus Product Listing Fetcher
 *
 * Fetches 2500 products from each of two categories (5000 total)
 * Downloads the listing data as JSON without fetching product details
 *
 * USAGE:
 * 1. Open browser console on Neiman Marcus website
 * 2. Paste this entire script
 * 3. The script will fetch products and download JSON file
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

  // Category IDs - Update these with your desired category IDs
  const CATEGORIES = [
    {
      id: "cat58290731", // First category (update if needed)
      name: "Category 1",
    },
    {
      id: "cat58290732", // Second category (UPDATE THIS with actual category ID)
      name: "Category 2",
    },
  ];

  const PRODUCTS_PER_CATEGORY = 6000;
  const DELAY_BETWEEN_PAGES = 1000; // 1 second delay

  /**
   * Fetch products from a single category (up to limit)
   */
  async function fetchProductsFromCategory(categoryId, categoryName, limit) {
    const all = [];
    let page = 1;
    let hasMore = true;

    console.log(
      `\n📦 Fetching products from ${categoryName} (${categoryId})...`
    );
    console.log(`   Target: ${limit} products\n`);

    while (hasMore && all.length < limit) {
      const listUrl = `https://www.neimanmarcus.com/c/dt/api/productlisting?categoryId=${categoryId}&page=${page}&parentCategoryId=&navPath=`;

      try {
        console.log(`   🔍 Fetching page ${page}...`);
        const res = await fetch(listUrl, { headers: HEADERS });

        if (!res.ok) {
          console.log(`   ⚠️ Page ${page} returned status ${res.status}`);
          hasMore = false;
          break;
        }

        const data = await res.json();
        const products = data?.products || [];

        if (products.length > 0) {
          // Calculate how many products we can add without exceeding limit
          const remaining = limit - all.length;
          const toAdd = Math.min(products.length, remaining);

          if (toAdd > 0) {
            all.push(...products.slice(0, toAdd));
            console.log(
              `   ✅ Page ${page}: Added ${toAdd} products (Total: ${all.length}/${limit})`
            );
          }

          // Check if we've reached the limit
          if (all.length >= limit) {
            console.log(
              `   ✅ Reached limit of ${limit} products for ${categoryName}`
            );
            hasMore = false;
            break;
          }

          // Check if there are more pages
          if (products.length < toAdd || products.length === 0) {
            hasMore = false;
          } else {
            page++;
            // Add delay between page requests to avoid rate limits
            await new Promise((r) => setTimeout(r, DELAY_BETWEEN_PAGES));
          }
        } else {
          console.log(`   ℹ️ No more products on page ${page}`);
          hasMore = false;
        }
      } catch (error) {
        console.log(`   ❌ Error fetching page ${page}:`, error.message);
        hasMore = false;
      }
    }

    console.log(`\n   ✅ ${categoryName}: Collected ${all.length} products`);
    return all;
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

  // Main execution
  console.log("🚀 Starting Neiman Marcus Product Listing Fetcher...");
  console.log(
    `📌 Fetching ${PRODUCTS_PER_CATEGORY} products from each category`
  );
  console.log(
    `📊 Total target: ${PRODUCTS_PER_CATEGORY * CATEGORIES.length} products\n`
  );

  const allProducts = [];
  const categoryResults = {};

  // Fetch products from each category
  for (const category of CATEGORIES) {
    const products = await fetchProductsFromCategory(
      category.id,
      category.name,
      PRODUCTS_PER_CATEGORY
    );

    categoryResults[category.id] = {
      categoryId: category.id,
      categoryName: category.name,
      productCount: products.length,
      products: products,
    };

    allProducts.push(...products);

    // Add delay between categories
    if (CATEGORIES.indexOf(category) < CATEGORIES.length - 1) {
      console.log(`\n⏳ Waiting 2 seconds before next category...\n`);
      await new Promise((r) => setTimeout(r, 2000));
    }
  }

  // Prepare final data structure
  const output = {
    metadata: {
      totalProducts: allProducts.length,
      fetchedAt: new Date().toISOString(),
      categories: CATEGORIES.map((cat) => ({
        id: cat.id,
        name: cat.name,
        productCount: categoryResults[cat.id]?.productCount || 0,
      })),
    },
    products: allProducts,
    byCategory: categoryResults,
  };

  // Download JSON file
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, -5);
  const filename = `neimanmarcus-product-listings-${allProducts.length}-products-${timestamp}.json`;

  console.log(`\n💾 Downloading JSON file: ${filename}`);
  downloadJSON(output, filename);

  console.log(`\n✅ Complete!`);
  console.log(`📊 Summary:`);
  console.log(`   Total products: ${allProducts.length}`);
  CATEGORIES.forEach((cat) => {
    const count = categoryResults[cat.id]?.productCount || 0;
    console.log(`   ${cat.name} (${cat.id}): ${count} products`);
  });
  console.log(`\n📁 File downloaded: ${filename}`);
})();
