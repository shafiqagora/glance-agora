/** @format */

const axios = require("axios");
const sanitizeHtml = require("sanitize-html");
const { HttpsProxyAgent } = require("https-proxy-agent");
const Anthropic = require("@anthropic-ai/sdk");
const puppeteer = require("puppeteer");

// Enhanced Proxy Rotation System for major providers
class ProxyRotator {
  constructor(config = {}) {
    this.config = config;
    this.proxies = [];

    this.setupProxies();
  }

  setupProxies() {
    const { provider, credentials } = this.config;

    if (provider === "decodo") {
      this.setupDecodo(credentials);
    } else {
      console.log("No valid proxy provider specified, running without proxies");
    }

    console.log(
      `Initialized ${this.proxies.length} proxies for provider: ${provider}`
    );
  }

  setupDecodo(credentials) {
    const { username, password, endpoint, port } = credentials;

    // Setup the rotating proxy
    if (port) {
      this.proxies.push({
        endpoint: `https://${username}:${password}@${endpoint}:${port}`,
        type: "decodo-rotating",
        port: port,
      });

      console.log(`Setup rotating Decodo proxy endpoint on port ${port}`);
    } else {
      console.log("No proxy port specified");
    }
  }

  // Method to get proxy based on country
  getProxyForCountry(country) {
    const { provider, credentials } = this.config;

    if (provider !== "decodo") {
      return this.getNextProxy();
    }

    // Determine endpoint based on country
    let endpoint;
    if (country === "US") {
      endpoint = "us.decodo.com";
    } else if (country === "IN") {
      endpoint = "in.decodo.com";
    } else {
      // Default to US proxy for other countries
      endpoint = "us.decodo.com";
    }

    // Create country-specific proxy configuration
    const countryProxy = {
      endpoint: `https://${credentials.username}:${credentials.password}@${endpoint}:${credentials.port}`,
      type: "decodo-rotating",
      port: credentials.port,
      country: country,
    };

    console.log(
      `🌍 Using ${country} proxy endpoint: ${endpoint} - ${this.maskCredentials(
        countryProxy.endpoint
      )}`
    );
    return countryProxy;
  }

  getNextProxy() {
    // Return the rotating proxy directly (we only have one proxy now)
    return this.proxies.length > 0 ? this.proxies[0] : null;
  }

  maskCredentials(endpoint) {
    return endpoint.replace(/\/\/[^@]+@/, "//***@");
  }

  getStats() {
    return this.proxies.map((proxy) => ({
      endpoint: this.maskCredentials(proxy.endpoint),
      type: proxy.type,
    }));
  }
}

// Configuration for your Decodo proxy provider
const proxyConfig = {
  provider: "decodo",
  credentials: {
    username: "splmzpsd06",
    password: "es7s2W=dDbn6rGy4En",
    endpoint: "us.decodo.com",
    port: 10000, // rotating proxy port
    isRotating: true, // Indicates this proxy rotates automatically
  },
};

const proxyRotator = new ProxyRotator(proxyConfig);

// Enhanced axios instance with proxy support
const createAxiosInstance = (proxy = null, timeout = 60000) => {
  const config = {
    timeout,
    headers: {
      "User-Agent": getRandomUserAgent(),
      Accept: "application/json, text/plain, */*",
      "Accept-Language": "en-US,en;q=0.9",
      "Accept-Encoding": "gzip, deflate, br",
      Connection: "keep-alive",
      "Cache-Control": "no-cache",
      Pragma: "no-cache",
    },
  };

  if (proxy?.endpoint) {
    try {
      config.httpsAgent = new HttpsProxyAgent(proxy.endpoint);
      config.httpAgent = new HttpsProxyAgent(proxy.endpoint);
      config.proxy = false; // Disable axios built-in proxy to use agent
    } catch (error) {
      console.log(
        `Error setting up proxy ${proxyRotator.maskCredentials(
          proxy.endpoint
        )}: ${error.message}`
      );
      return createAxiosInstance(); // Fallback to no proxy
    }
  }

  return axios.create(config);
};

// Rotating user agents
const userAgents = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
];

const getRandomUserAgent = () => {
  return userAgents[Math.floor(Math.random() * userAgents.length)];
};

// Enhanced retry with intelligent proxy rotation
async function retryRequestWithProxyRotation(
  requestFunc,
  maxRetries = 5,
  baseDelay = 2000,
  country = "US"
) {
  let attempt = 0;
  let currentProxy = null;

  while (attempt < maxRetries) {
    try {
      // Get country-specific proxy
      currentProxy = proxyRotator.getProxyForCountry(country);
      if (currentProxy) {
        console.log(
          `Using proxy: ${proxyRotator.maskCredentials(currentProxy.endpoint)}`
        );
      }

      const axiosInstance = createAxiosInstance(currentProxy);
      const response = await requestFunc(axiosInstance);

      return response;
    } catch (error) {
      attempt++;

      // Handle 429 errors with exponential backoff
      if (error.response?.status === 429) {
        const retryAfter = error.response.headers["retry-after"];
        const delay = retryAfter
          ? Math.min(parseInt(retryAfter) * 1000, 30000)
          : Math.min(baseDelay * Math.pow(2, attempt), 30000);

        console.log(
          `Rate limited (429) - waiting ${delay}ms. Attempt ${attempt}/${maxRetries}`
        );

        if (attempt < maxRetries) {
          await new Promise((resolve) => setTimeout(resolve, delay));
          continue;
        }
      }

      // Handle 404 errors
      if (error.response?.status === 404) {
        const delay = Math.min(baseDelay * attempt, 5000);
        console.log(`404 Not Found. Attempt ${attempt}/${maxRetries}`);

        if (attempt < maxRetries) {
          await new Promise((resolve) => setTimeout(resolve, delay));
          continue;
        }
      }

      // Handle timeout errors specifically
      if (error.code === "ECONNABORTED" && error.message.includes("timeout")) {
        const delay = Math.min(baseDelay * Math.pow(2, attempt), 15000);
        console.log(
          `Request timeout (${error.message}) - waiting ${delay}ms. Attempt ${attempt}/${maxRetries}`
        );

        if (attempt < maxRetries) {
          await new Promise((resolve) => setTimeout(resolve, delay));
          continue;
        }
      }

      // Handle connection errors
      if (
        error.code === "ECONNRESET" ||
        error.code === "ETIMEDOUT" ||
        error.code === "ENOTFOUND" ||
        error.response?.status >= 500
      ) {
        const delay = Math.min(baseDelay * attempt, 5000);
        console.log(
          `Connection error: ${
            error.code || error.response?.status
          }. Attempt ${attempt}/${maxRetries}`
        );

        if (attempt < maxRetries) {
          await new Promise((resolve) => setTimeout(resolve, delay));
          continue;
        }
      }

      // For other errors, retry with delay
      if (error.response?.status && error.response.status < 500) {
        const delay = Math.min(baseDelay * attempt, 3000);
        console.log(
          `HTTP error ${error.response.status} - retrying. Attempt ${attempt}/${maxRetries}`
        );

        if (attempt < maxRetries) {
          await new Promise((resolve) => setTimeout(resolve, delay));
          continue;
        }
      }

      if (attempt === maxRetries) {
        throw error;
      }
    }
  }
}

async function useDigitalOceanAI(messages, options = {}) {
  const { maxTokens = 1000, temperature = 0.7 } = options;

  try {
    const response = await fetch(
      "https://k6qoaxc3ustu5xxybjfk4j2i.agents.do-ai.run/api/v1/chat/completions",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer Yikal4gYHnhCkgn3SSNMe95s_VQNUd3K`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          messages,
          max_tokens: maxTokens,
          temperature,
          stream: false,
        }),
      }
    );

    if (!response.ok) {
      const errorData = await response.json();
      throw new Error(
        `DigitalOcean AI API error: ${errorData.message || response.statusText}`
      );
    }

    return await response.json();
  } catch (error) {
    console.error("Error using DigitalOcean AI:", error);
    throw error;
  }
}

// Initialize Anthropic client
const anthropic = new Anthropic({
  apiKey: process.env.ANTHROPIC_API_KEY || "your-anthropic-api-key-here",
});

async function useClaude3Haiku(messages, options = {}) {
  const { maxTokens = 1000, temperature = 0.7 } = options;

  try {
    const response = await anthropic.messages.create({
      model: "claude-3-haiku-20240307",
      max_tokens: maxTokens,
      temperature,
      messages,
    });

    return response;
  } catch (error) {
    console.error("Error using Claude 3 Haiku:", error);
    throw error;
  }
}

// Clean and truncate HTML content
const cleanAndTruncate = (html) => {
  const cleaned = sanitizeHtml(html, {
    allowedTags: [],
    allowedAttributes: {},
  }); // Remove all HTML tags
  return cleaned.length > 5000 ? cleaned.slice(0, 5000) : cleaned; // Limit to 5000 characters
};

async function fetchProductVariants(productUrl, country = "US") {
  try {
    const response = await retryRequestWithProxyRotation(
      async (axiosInstance) => {
        return await axiosInstance.get(`${productUrl}.js`);
      },
      5,
      2000,
      country
    );

    return response.data?.variants || [];
  } catch (error) {
    console.error(`Error fetching variants for ${productUrl}:`, error.message);
    return [];
  }
}

function getDomainName(inputUrl) {
  try {
    const parsedUrl = new URL(inputUrl);
    let domain = parsedUrl.hostname;

    if (!domain) {
      domain = inputUrl;
    }

    if (domain.startsWith("www.")) {
      domain = domain.substring(4);
    }

    return domain;
  } catch (error) {
    let domain = inputUrl;
    if (domain.startsWith("www.")) {
      domain = domain.substring(4);
    }
    return domain;
  }
}

function calculateDiscount(originalPrice, finalPrice) {
  if (!originalPrice || !finalPrice || originalPrice <= finalPrice) return 0;
  return Math.round(((originalPrice - finalPrice) / originalPrice) * 100);
}

function extractSize(variant, productOptions) {
  // Map variant options to product option definitions
  for (let i = 1; i <= 3; i++) {
    const optionValue = variant[`option${i}`];
    if (optionValue && optionValue !== "Default Title") {
      // Find the corresponding product option by position
      const productOption = productOptions?.find((opt) => opt.position === i);
      if (productOption && productOption.name.toLowerCase().includes("size")) {
        return optionValue;
      }
    }
  }

  // Fallback: if no size option found, try to detect from variant title/options using original logic
  for (let i = 1; i <= 3; i++) {
    const option = variant[`option${i}`];
    if (option && option !== "Default Title") {
      // If it looks like a size (S, M, L, XL, numbers, etc.)
      if (/^(XXS|XS|S|M|L|XL|XXL|XXXL|\d+W?(\.\d+)?|\d+\/\d+)$/i.test(option)) {
        return option;
      }
    }
  }

  return ""; // Return empty string instead of 'One Size'
}

function extractColor(variant, productOptions) {
  // Map variant options to product option definitions
  for (let i = 1; i <= 3; i++) {
    const optionValue = variant[`option${i}`];
    if (optionValue && optionValue !== "Default Title") {
      // Find the corresponding product option by position
      const productOption = productOptions?.find((opt) => opt.position === i);
      if (productOption && productOption.name.toLowerCase().includes("color")) {
        return optionValue;
      }
    }
  }

  return ""; // Return empty string instead of 'Default'
}

// Use Claude 3 Haiku to determine gender and materials from product details
async function determineProductDetails(product) {
  try {
    const messages = [
      {
        role: "user",
        content: `You are an expert product analyst. Based on product information, determine if this is a clothing/apparel product and if so, determine the gender target, materials, and category.

IMPORTANT: First check if this is a clothing/apparel product (clothing, shoes, accessories, fashion items). If it's NOT clothing/apparel (like electronics, home goods, beauty products, books, etc.), return "SKIP_PRODUCT" as the category.

Respond ONLY with a JSON object in this exact format: {"gender": "Male|Female|Kids|Unisex", "materials": "material description", "category": "category name or SKIP_PRODUCT"}

Be specific about materials if mentioned in the description, otherwise provide a reasonable estimate based on the product type. For category, use clear, descriptive category names like "T-Shirts", "Dresses", "Shoes", "Jeans", "Jackets", etc.

Analyze this product:
Title: ${product.title}
Type: ${product.product_type || "Not specified"}
Description: ${product.description || "No description available"}
Tags: ${product.tags || "No tags"}
Vendor: ${product.vendor || "Unknown"}

Determine if this is clothing/apparel, and if so, the target gender, materials, and category.`,
      },
    ];

    const response = await useClaude3Haiku(messages, {
      maxTokens: 250,
      temperature: 0.3,
    });

    const content = response.content?.[0]?.text;

    if (content) {
      try {
        const parsed = JSON.parse(content);

        // Check if product should be skipped
        if (parsed.category === "SKIP_PRODUCT") {
          return {
            shouldSkip: true,
            reason: "Product is not clothing/apparel",
          };
        }

        return {
          shouldSkip: false,
          gender: parsed.gender || "Unisex",
          materials: "",
          category: parsed.category || "",
        };
      } catch (parseError) {
        console.log("Failed to parse AI response, using fallback logic");
        return getFallbackProductDetails(product);
      }
    } else {
      return getFallbackProductDetails(product);
    }
  } catch (error) {
    console.log("AI request failed, using fallback logic:", error.message);
    return getFallbackProductDetails(product);
  }
}

// Use Claude 3 Haiku to determine gender and materials from product details
async function determineProductDetailsNYDJ(product) {
  try {
    // Allowed categories list for NYDJ
    const allowedCategories = [
      "Jeans",
      "Blouses",
      "Pants",
      "Tees",
      "Shirts",
      "Sweaters",
      "Jackets",
      "Denim Shorts",
      "Non Denim Shorts",
      "Leggings",
      "Handbags",
      "Accessories",
    ];

    const messages = [
      {
        role: "user",
        content: `You are an expert product analyst. Based on product information, determine the gender target, materials, and category. 

IMPORTANT: The category MUST be one of these exact options: ${allowedCategories.join(
          ", "
        )}.
If the product doesn't fit into any of these categories, return "SKIP_PRODUCT" as the category.

Respond ONLY with a JSON object in this exact format: {"gender": "Male|Female|Kids|Unisex", "materials": "material description", "category": "category name or SKIP_PRODUCT"}

Analyze this product:
Title: ${product.title}
Type: ${product.product_type || "Not specified"}
Description: ${product.description || "No description available"}
Tags: ${product.tags || "No tags"}
Vendor: ${product.vendor || "Unknown"}

Determine the target gender (Male/Female/Kids/Unisex), materials, and category for this product.`,
      },
    ];

    const response = await useClaude3Haiku(messages, {
      maxTokens: 250,
      temperature: 0.3,
    });

    const content = response.content?.[0]?.text;

    if (content) {
      try {
        const parsed = JSON.parse(content);

        // Check if product should be skipped
        if (parsed.category === "SKIP_PRODUCT") {
          return {
            shouldSkip: true,
            reason: "Product category not in allowed list",
          };
        }

        // Validate category is in allowed list
        if (!allowedCategories.includes(parsed.category)) {
          return {
            shouldSkip: true,
            reason: `Category "${parsed.category}" not in allowed list`,
          };
        }

        return {
          shouldSkip: false,
          gender: parsed.gender || "Unisex",
          materials: "",
          category: parsed.category || "",
        };
      } catch (parseError) {
        console.log("Failed to parse AI response, using fallback logic");
        return getFallbackProductDetailsNYDJ(product);
      }
    } else {
      return getFallbackProductDetailsNYDJ(product);
    }
  } catch (error) {
    console.log("AI request failed, using fallback logic:", error.message);
    return getFallbackProductDetailsNYDJ(product);
  }
}

// Fallback function for when AI fails
function getFallbackProductDetails(product) {
  const text =
    `${product.product_type} ${product.title} ${product.tags}`.toLowerCase();

  // Check if it's likely clothing/apparel based on keywords
  const clothingKeywords = [
    "clothing",
    "shirt",
    "dress",
    "pants",
    "jeans",
    "jacket",
    "coat",
    "sweater",
    "hoodie",
    "blouse",
    "skirt",
    "shorts",
    "shoes",
    "sneakers",
    "boots",
    "sandals",
    "hat",
    "cap",
    "bag",
    "purse",
    "belt",
    "scarf",
    "gloves",
    "socks",
    "underwear",
    "bra",
    "swimwear",
    "bikini",
    "tee",
    "t-shirt",
    "polo",
    "cardigan",
    "vest",
    "leggings",
    "tights",
    "pajamas",
    "nightwear",
    "activewear",
    "sportswear",
    "apparel",
    "fashion",
    "wear",
    "outfit",
  ];

  const isClothing = clothingKeywords.some((keyword) => text.includes(keyword));

  if (!isClothing) {
    return {
      shouldSkip: true,
      reason: "Product does not appear to be clothing/apparel (fallback)",
    };
  }

  let gender = "Unisex";
  if (text.includes("men") && !text.includes("women")) gender = "Male";
  else if (text.includes("women") || text.includes("ladies")) gender = "Female";
  else if (
    text.includes("kids") ||
    text.includes("children") ||
    text.includes("baby")
  )
    gender = "Kids";

  return {
    shouldSkip: false,
    gender,
    materials: "100% Cotton", // Default fallback
    category: "",
  };
}

// Fallback function for NYDJ when AI fails
function getFallbackProductDetailsNYDJ(product) {
  const allowedCategories = [
    "Jeans",
    "Blouses",
    "Pants",
    "Tees",
    "Shirts",
    "Sweaters",
    "Jackets",
    "Denim Shorts",
    "Non Denim Shorts",
    "Leggings",
    "Handbags",
    "Accessories",
  ];

  const text =
    `${product.product_type} ${product.title} ${product.tags}`.toLowerCase();

  let gender = "Unisex";
  if (text.includes("men") && !text.includes("women")) gender = "Male";
  else if (text.includes("women") || text.includes("ladies")) gender = "Female";
  else if (
    text.includes("kids") ||
    text.includes("children") ||
    text.includes("baby")
  )
    gender = "Kids";

  // Try to determine category based on keywords
  let category = "";
  if (text.includes("jean") || text.includes("denim jean")) category = "Jeans";
  else if (text.includes("blouse")) category = "Blouses";
  else if (text.includes("pant") && !text.includes("jean")) category = "Pants";
  else if (text.includes("tee") || text.includes("t-shirt")) category = "Tees";
  else if (text.includes("shirt") && !text.includes("t-shirt"))
    category = "Shirts";
  else if (text.includes("sweater") || text.includes("pullover"))
    category = "Sweaters";
  else if (text.includes("jacket") || text.includes("coat"))
    category = "Jackets";
  else if (text.includes("short") && text.includes("denim"))
    category = "Denim Shorts";
  else if (text.includes("short") && !text.includes("denim"))
    category = "Non Denim Shorts";
  else if (text.includes("legging")) category = "Leggings";
  else if (
    text.includes("handbag") ||
    text.includes("purse") ||
    text.includes("bag")
  )
    category = "Handbags";
  else if (
    text.includes("accessory") ||
    text.includes("jewelry") ||
    text.includes("belt") ||
    text.includes("scarf")
  )
    category = "Accessories";

  // If category is not in allowed list, skip the product
  if (!category || !allowedCategories.includes(category)) {
    return {
      shouldSkip: true,
      reason: "Product category not in allowed list (fallback)",
    };
  }

  return {
    shouldSkip: false,
    gender,
    materials: "100% Cotton", // Default fallback
    category,
  };
}

// Function to retry Puppeteer requests with proxy rotation
async function retryPuppeteerWithProxyRotation(
  requestFunc,
  maxRetries = 3,
  baseDelay = 2000,
  country = "US",
  storeUrl = ""
) {
  let attempt = 0;

  while (attempt < maxRetries) {
    let browser = null;
    try {
      attempt++;

      // Get country-specific proxy
      const currentProxy = proxyRotator.getProxyForCountry(country);

      // Determine endpoint based on country
      let endpoint;
      if (country === "US") {
        endpoint = "us.decodo.com";
      } else if (country === "IN") {
        endpoint = "in.decodo.com";
      } else {
        endpoint = "us.decodo.com"; // Default to US
      }

      const proxyConfig = {
        provider: "decodo",
        credentials: {
          username: process.env.PROXY_USERNAME || "splmzpsd06",
          password: process.env.PROXY_PASSWORD || "es7s2W=dDbn6rGy4En",
          endpoint: endpoint,
          port: process.env.PROXY_PORT || 10000,
          isRotating: true,
        },
      };

      // Create proxy URL for Puppeteer
      const proxyUrl = `http://${proxyConfig.credentials.username}:${proxyConfig.credentials.password}@${proxyConfig.credentials.endpoint}:${proxyConfig.credentials.port}`;

      console.log(
        `🌍 Puppeteer using ${country} proxy endpoint: ${proxyConfig.credentials.endpoint}`
      );

      // Launch browser with proxy
      browser = await puppeteer.launch({
        headless: true,
        args: [
          "--no-sandbox",
          "--disable-setuid-sandbox",
          `--proxy-server=${proxyUrl}`,
          "--disable-dev-shm-usage",
          "--disable-features=VizDisplayCompositor",
        ],
      });

      const result = await requestFunc(browser);

      if (browser) {
        await browser.close();
      }

      return result;
    } catch (error) {
      if (browser) {
        try {
          await browser.close();
        } catch (closeError) {
          console.log("Error closing browser:", closeError.message);
        }
      }

      console.log(`Attempt ${attempt} failed:`, storeUrl, error.message);

      if (attempt === maxRetries) {
        throw error;
      }

      // Wait before retry with exponential backoff
      const delay = Math.min(baseDelay * Math.pow(2, attempt - 1), 15000);
      console.log(`Waiting ${delay}ms before retry...`);
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }
}

module.exports = {
  useDigitalOceanAI,
  useClaude3Haiku,
  cleanAndTruncate,
  fetchProductVariants,
  getDomainName,
  calculateDiscount,
  extractSize,
  extractColor,
  determineProductDetails,
  determineProductDetailsNYDJ,
  getFallbackProductDetails,
  getFallbackProductDetailsNYDJ,
  retryRequestWithProxyRotation,
  createAxiosInstance,
  retryPuppeteerWithProxyRotation,
};
