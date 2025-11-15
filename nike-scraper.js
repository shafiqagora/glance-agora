/** @format */

// Nike Products Scraper - Men's and Women's Categories
// Uses fetch API to scrape products from Nike.com
require("dotenv").config();
const fs = require("fs");
const sanitizeHtml = require("sanitize-html");
const { v4: uuidv4, v5: uuidv5 } = require("uuid");
const zlib = require("zlib");
const path = require("path");

// Import helper functions and database
const { connectDB, disconnectDB } = require("./database/connection");
const Product = require("./models/Product");
const Store = require("./models/Store");
const {
  calculateDiscount,
  extractSize,
  extractColor,
  determineProductDetails,
  cleanAndTruncate,
  getDomainName,
} = require("./utils/helper");

// Helper function to get product availability information
const getProductAvailability = async (groupKey) => {
  try {
    const availabilityUrl = `https://api.nike.com/discover/product_details_availability/v1/marketplace/US/language/en/consumerChannelId/d9a5bc42-4b9c-4976-858a-f159cf99c647/groupKey/${groupKey}`;
    console.log(`Fetching availability info for group: ${groupKey}`);

    const response = await fetch(availabilityUrl, {
      method: "GET",
      headers: {
        accept: "*/*",
        "accept-language": "en-US,en;q=0.9",
        "nike-api-caller-id": "com.nike.commerce.nikedotcom.web",
        origin: "https://www.nike.com",
        priority: "u=1, i",
        referer: "https://www.nike.com/",
        "sec-ch-ua":
          '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent":
          "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
      },
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const availabilityData = await response.json();
    return availabilityData;
  } catch (error) {
    console.error(
      `Error fetching product availability for ${groupKey}:`,
      error.message
    );
    return null;
  }
};

// Helper function to process a single product
const processProduct = async (product) => {
  const firstProduct = product.products[0];
  const productId = firstProduct.productCode;
  console.log(`Processing product: ${firstProduct.copy.title}`);

  // Clean description
  let description = "";

  // Extract materials from product details
  let materials = "";
  if (firstProduct.featuredAttributes) {
    materials = firstProduct.featuredAttributes[0];
  }

  const formattedProduct = {
    parent_product_id: productId,
    name: firstProduct.copy.title,
    description: description,
    category: firstProduct.productType,
    retailer_domain: "nike.com",
    brand: "Nike",
    gender: product.gender,
    materials: materials,
    return_policy_link: "https://www.nike.com/help/a/returns-policy",
    return_policy: "",
    size_chart: "https://www.nike.com/help/size-charts",
    available_bank_offers: "",
    available_coupons: "",
    variants: [],
    operation_type: "INSERT",
    source: "nike",
  };
  const productAvailability = await getProductAvailability(
    firstProduct.groupKey
  );

  // Process variants (colors and sizes)
  if (product.products.length > 0) {
    for (const variant of product.products) {
      // Get color information
      const colorName = variant.displayColors.colorDescription;

      // Get pricing information
      const originalPrice = parseFloat(variant.prices.initialPrice || 0);
      const salePrice = parseFloat(variant.prices.currentPrice || 0);
      const finalPrice = salePrice > 0 ? salePrice : originalPrice;
      const discount = variant.prices.discountPercentage;
      const isOnSale = salePrice > 0 && salePrice < originalPrice;

      // Get images
      let imageUrl = variant.colorwayImages?.portraitURL || "";
      let alternateImages = variant.colorwayImages?.squarishURL || [];

      // Get sizes from availability data
      const sizes =
        productAvailability?.sizes.filter(
          (item) => item.productCode == variant.productCode
        ) || [];

      // Get product availability information

      if (sizes.length > 0) {
        for (const size of sizes) {
          const isInStock = size.availability.isAvailable;
          let variantUrl = variant.pdpUrl.url;
          variantUrl = variantUrl.split("/").slice(0, -1).join("/");
          variantUrl = `${variantUrl}/${variant.productCode}`;
          const formattedVariant = {
            price_currency: "USD",
            original_price: originalPrice,
            link_url: variantUrl,
            deeplink_url: variantUrl,
            image_url: imageUrl,
            alternate_image_urls: alternateImages,
            is_on_sale: isOnSale,
            is_in_stock: isInStock,
            size: size.label,
            color: colorName,
            mpn: uuidv5(
              `${variant.groupKey}-${colorName}`,
              "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
            ),
            ratings_count: 0,
            average_ratings: 0,
            review_count: 0,
            selling_price: originalPrice,
            sale_price: salePrice > 0 ? salePrice : 0,
            final_price: finalPrice,
            discount: discount,
            operation_type: "INSERT",
            variant_id: uuidv5(
              `${variant.productCode}-${size.skuId}-${size.label}`,
              "6ba7b810-9dad-11d1-80b4-00c04fd430c1"
            ),
            variant_description: "",
          };
          formattedProduct.variants.push(formattedVariant);
        }
      }
    }
  }

  const mongoResult = await saveProductToMongoDB(formattedProduct);

  return { formattedProduct, mongoResult };
};

// Save product to MongoDB
async function saveProductToMongoDB(productData) {
  try {
    // Create new product with INSERT operation type
    productData.operation_type = "INSERT";
    productData.variants.forEach((variant) => {
      variant.operation_type = "INSERT";
    });

    const newProduct = new Product(productData);
    await newProduct.save();
    console.log(`✅ Saved to MongoDB: ${productData.name}`);
    return { operation: "INSERT", product: newProduct };
  } catch (error) {
    console.error(
      `❌ Error saving product ${productData.name} to MongoDB:`,
      error.message
    );
    return { operation: "ERROR", error: error.message };
  }
}

// Save or update store entry with product IDs
async function saveStoreEntry(storeData, productIds) {
  try {
    // Check if store already exists
    let existingStore = await Store.findOne({
      storeType: "nike",
      name: "Nike",
      country: storeData.country || "US",
    });

    if (existingStore) {
      console.log("Store already exists, updating with new products...");
      // Add new product IDs to existing store (avoid duplicates)
      const existingProductIds = existingStore.products.map((id) =>
        id.toString()
      );
      const newProductIds = productIds.filter(
        (id) => !existingProductIds.includes(id.toString())
      );

      existingStore.products.push(...newProductIds);
      existingStore.isScrapped = true;
      existingStore.updatedAt = new Date();

      await existingStore.save();
      console.log(`✅ Updated store with ${newProductIds.length} new products`);
      return { operation: "UPDATED", store: existingStore };
    } else {
      // Create new store entry
      const newStore = new Store({
        products: productIds,
        name: storeData.name || "Nike",
        storeTemplate: "nike-template",
        storeType: "nike",
        storeUrl: "https://www.nike.com",
        city: "",
        state: "",
        country: storeData.country || "US",
        isScrapped: true,
        returnPolicy: "https://www.nike.com/help/a/returns-policy",
        tags: ["men", "women", "sports", "clothing", "footwear"],
      });

      await newStore.save();
      console.log(`✅ Created new store with ${productIds.length} products`);
      return { operation: "CREATED", store: newStore };
    }
  } catch (error) {
    console.error("❌ Error saving store entry:", error.message);
    return { operation: "ERROR", error: error.message };
  }
}

// Helper function to scrape products from a specific category
async function scrapeNikeCategory(categoryConfig, targetProductCount = 2500) {
  let anchor = 0;
  let allProducts = [];
  let hasMoreProducts = true;
  const pageSize = 24; // Nike uses 24 products per page

  console.log(`\n🎯 Starting to scrape ${categoryConfig.name} category...`);
  console.log(`Target: ${targetProductCount} products`);

  while (hasMoreProducts && allProducts.length < targetProductCount) {
    console.log(
      `\nFetching ${categoryConfig.name} starting from anchor ${anchor}...`
    );

    try {
      // Build the API URL
      const apiUrl = `https://api.nike.com/discover/product_wall/v1/marketplace/US/language/en/consumerChannelId/d9a5bc42-4b9c-4976-858a-f159cf99c647?path=${categoryConfig.path}&attributeIds=${categoryConfig.attributeIds}&queryType=PRODUCTS&anchor=${anchor}&count=${pageSize}`;

      console.log(`API URL: ${apiUrl}`);

      // Fetch data from API
      const response = await fetch(apiUrl, {
        method: "GET",
        headers: {
          accept: "*/*",
          "accept-language": "en-US,en;q=0.9",
          anonymousid: "86D5EFDB7D6D5CE3E943F2971F2278EA",
          "nike-api-caller-id": "nike:dotcom:browse:wall.client:2.0",
          origin: "https://www.nike.com",
          priority: "u=1, i",
          referer: "https://www.nike.com/",
          "sec-ch-ua":
            '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
          "sec-ch-ua-mobile": "?0",
          "sec-ch-ua-platform": '"macOS"',
          "sec-fetch-dest": "empty",
          "sec-fetch-mode": "cors",
          "sec-fetch-site": "same-site",
          "user-agent":
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
        },
        credentials: "include", // This handles the cookies from the curl command
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const pageData = await response.json();

      // Extract products from the response
      let pageProducts = [];
      if (pageData?.productGroupings?.length > 0) {
        pageProducts = pageData.productGroupings;
        console.log(
          `Found ${pageProducts.length} products on ${categoryConfig.name} page starting from anchor ${anchor}`
        );
        allProducts = allProducts.concat(pageProducts);
        console.log(
          `${categoryConfig.name} total products so far: ${allProducts.length}`
        );

        // Check if we have more products
        if (pageProducts.length < pageSize) {
          hasMoreProducts = false;
        } else {
          anchor += pageSize;
        }
      } else {
        console.log(
          `No products found on ${categoryConfig.name} page starting from anchor ${anchor}`
        );
        hasMoreProducts = false;
      }

      // Add a small delay to be respectful to the API
      await new Promise((resolve) => setTimeout(resolve, 1000));
    } catch (error) {
      console.error(
        `Error fetching ${categoryConfig.name} page starting from anchor ${anchor}:`,
        error.message
      );
      hasMoreProducts = false;
    }
  }

  // Limit to target product count
  if (allProducts.length > targetProductCount) {
    allProducts = allProducts.slice(0, targetProductCount);
  }

  console.log(`\n✅ ${categoryConfig.name} scraping completed!`);
  console.log(
    `📦 ${categoryConfig.name} total products collected: ${allProducts.length}`
  );

  return allProducts;
}

// Generate files for combined products from all categories
async function generateCombinedFiles(products, storeData) {
  const countryCode = storeData.country || "US";
  const formattedProducts = [];
  const productIds = []; // Track product IDs for store entry
  const mongoResults = {
    inserted: 0,
    skipped: 0,
    errors: 0,
  };

  // Process products sequentially
  console.log(
    `\n📦 Processing ${products.length} products from all categories...`
  );

  for (let i = 0; i < products.length; i++) {
    const product = products[i];
    const gender = product._gender || "";
    const category = product._category || "";

    console.log(
      `Processing ${category} product ${i + 1}/${products.length}: ${
        product.title || product.name
      }`
    );

    try {
      const result = await processProduct(product, gender, category);

      if (result.formattedProduct) {
        formattedProducts.push(result.formattedProduct);

        // Track product ID for store entry
        if (result.mongoResult.product) {
          productIds.push(result.mongoResult.product._id);
        }

        if (result.mongoResult.operation === "INSERT") {
          mongoResults.inserted++;
        } else if (result.mongoResult.operation === "SKIPPED") {
          mongoResults.skipped++;
        } else {
          mongoResults.errors++;
        }
      }

      // Add a small delay between products
      await new Promise((resolve) => setTimeout(resolve, 500));
    } catch (error) {
      console.error(
        `Error processing product ${product.title || product.name}:`,
        error.message
      );
      mongoResults.errors++;
    }
  }

  // Create directory structure: countryCode/retailername-countryCode-combined/
  const cleanBrandName = "nike";
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
      name: storeData.name || "Nike",
      domain: "nike.com",
      currency: storeData.currency || "USD",
      country: countryCode,
      total_products: formattedProducts.length,
      categories: ["Men", "Women"],
      crawled_at: new Date().toISOString(),
    },
    products: formattedProducts,
  };

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

  // Create or update store entry with all product IDs
  console.log("\n📦 Creating/updating store entry...");
  const storeResult = await saveStoreEntry(storeData, productIds);

  // Log MongoDB results
  console.log(`\n📊 MongoDB Results:`);
  console.log(`  Products inserted: ${mongoResults.inserted}`);
  console.log(`  Products skipped: ${mongoResults.skipped}`);
  console.log(`  Products errors: ${mongoResults.errors}`);
  console.log(`  Store operation: ${storeResult.operation}`);

  return {
    jsonPath: gzippedFilePath,
    mongoResults,
    storeResult,
    totalProductIds: productIds.length,
  };
}

async function scrapeNikeProducts() {
  try {
    // Connect to MongoDB
    await connectDB();

    console.log("🚀 Starting Nike scraping using fetch API...");

    // Define categories to scrape - Nike category structure
    const categories = [
      {
        name: "Men Clothing",
        gender: "Men",
        path: "/w/mens-clothing-6ymx6znik1",
        attributeIds:
          "a00f0bb2-648b-4853-9559-4cd943b7d6c6,0f64ecc7-d624-4e91-b171-b83a03dd8550",
      },
      {
        name: "Women's Clothing",
        gender: "Women",
        path: "/w/womens-clothing-5e1x6z6ymx6",
        attributeIds:
          "7baf216c-acc6-4452-9e07-39c2ca77ba32,a00f0bb2-648b-4853-9559-4cd943b7d6c6",
      },
      {
        name: "Kids's Clothing",
        gender: "Women",
        path: "/w/kids-clothing-6ymx6zv4dh",
        attributeIds:
          "a00f0bb2-648b-4853-9559-4cd943b7d6c6,145ce13c-5740-49bd-b2fd-0f67214765b3",
      },
    ];

    const targetProductsPerCategory = 1000;
    const allResults = [];

    const storeData = {
      name: "Nike",
      domain: "nike.com",
      currency: "USD",
      country: "US",
    };

    // Collect all products from all categories
    let allProducts = [];
    let allProductDetails = [];

    // Scrape each category
    for (const category of categories) {
      console.log(`\n${"=".repeat(50)}`);
      console.log(`🎯 Starting ${category.name} category scraping`);
      console.log(`${"=".repeat(50)}`);

      const categoryProducts = await scrapeNikeCategory(
        category,
        targetProductsPerCategory
      );

      if (categoryProducts.length === 0) {
        console.log(`⚠️ No products found for ${category.name} category`);
        continue;
      }

      console.log(
        `\n📦 Found ${categoryProducts.length} ${category.name} products`
      );

      // Add category info to each product for processing
      const categoryProductsWithGender = categoryProducts.map((product) => ({
        ...product,
        _category: category.name,
        _gender: category.gender,
      }));

      allProducts = allProducts.concat(categoryProductsWithGender);

      allProductDetails.push({
        category: category.name,
        gender: category.gender,
        count: categoryProducts.length,
      });
    }

    console.log(`\n${"🎯".repeat(20)}`);
    console.log("🎯 PROCESSING ALL PRODUCTS TOGETHER 🎯");
    console.log(`${"🎯".repeat(20)}`);
    console.log(`📦 Total products collected: ${allProducts.length}`);
    allProductDetails.forEach((detail) => {
      console.log(`   ${detail.category}: ${detail.count} products`);
    });

    if (allProducts.length === 0) {
      console.log("⚠️ No products found from any category");
      return false;
    }

    // Process all products together and generate combined files
    const combinedFilesResult = await generateCombinedFiles(
      allProducts,
      storeData
    );

    allResults.push({
      categories: allProductDetails,
      totalProducts: allProducts.length,
      jsonPath: combinedFilesResult.jsonPath,
      mongoResults: combinedFilesResult.mongoResults,
      storeResult: combinedFilesResult.storeResult,
      totalProductIds: combinedFilesResult.totalProductIds,
    });

    console.log(`\n${"🎉".repeat(20)}`);
    console.log("🎉 ALL NIKE SCRAPING COMPLETED SUCCESSFULLY! 🎉");
    console.log(`${"🎉".repeat(20)}`);

    // Summary for combined results
    const combinedResult = allResults[0];
    console.log(`\n📊 Combined Results Summary:`);
    console.log(`   Total Products: ${combinedResult.totalProducts}`);
    console.log(`   Categories Processed:`);
    combinedResult.categories.forEach((cat) => {
      console.log(`     ${cat.category}: ${cat.count} products`);
    });
    console.log(`   Output Files: ${combinedResult.jsonPath}`);
    console.log(
      `   MongoDB - Inserted: ${combinedResult.mongoResults.inserted}, Skipped: ${combinedResult.mongoResults.skipped}, Errors: ${combinedResult.mongoResults.errors}`
    );
    console.log(`   Store Operation: ${combinedResult.storeResult.operation}`);
    console.log(`   Total Product IDs: ${combinedResult.totalProductIds}`);

    return allResults;
  } catch (error) {
    console.error("❌ Error during scraping:", error);
    throw error;
  } finally {
    // Disconnect from MongoDB
    await disconnectDB();
  }
}

// Export the main function for use in other modules
const main = async () => {
  try {
    const results = await scrapeNikeProducts();

    if (results && results.length > 0) {
      console.log("\n🎉 Nike products crawling completed successfully!");

      let totalProducts = 0;
      let totalInserted = 0;
      let totalSkipped = 0;
      let totalErrors = 0;

      results.forEach((res) => {
        totalProducts += res.totalProducts;
        totalInserted += res.mongoResults.inserted;
        totalSkipped += res.mongoResults.skipped;
        totalErrors += res.mongoResults.errors;
      });

      console.log(`📊 Final Summary:`);
      console.log(`   Total products: ${totalProducts}`);
      console.log(
        `   MongoDB - Inserted: ${totalInserted}, Skipped: ${totalSkipped}, Errors: ${totalErrors}`
      );

      // Show category breakdown
      if (results[0] && results[0].categories) {
        console.log(`   Categories:`);
        results[0].categories.forEach((cat) => {
          console.log(`     ${cat.category}: ${cat.count} products`);
        });
      }

      return results;
    } else {
      console.log("\n❌ Nike crawling failed");
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

module.exports = { main, scrapeNikeProducts };
