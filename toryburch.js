/** @format */

const fs = require("fs");
const path = require("path");
const zlib = require("zlib");
const { v5: uuidv5 } = require("uuid");

const COLOR_NAMESPACE = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
const VARIANT_NAMESPACE = "6ba7b810-9dad-11d1-80b4-00c04fd430c1";

const STORE_DETAILS = {
  retailer_domain: "toryburch.com",
  brand: "Tory Burch",
  return_policy_link:
    "https://www.toryburch.com/en-us/client-services/shipping/shipping-and-delivery/",
  return_policy:
    "We offer free shipping and free returns within the continental U.S., automatically applied during checkout. See Shipping Details. Complimentary gift wrap is also available at checkout.",
  size_chart: "https://www.toryburch.com/en-us/size-guide/",
  currency: "USD",
  country: "US",
};

// All clothing categories to scrape
const CLOTHING_CATEGORIES = [
  "clothing-dresses",
  "clothing-sweaters",
  "clothing-tops",
  "clothing-pants",
  "clothing-skirts",
  "clothing-jackets-outerwear",
  "swim-view-all",
];

const API_BASE_URL = "https://www.toryburch.com/api/prod-r2/v11/categories";
const API_PARAMS = "site=ToryBurch_US&locale=en-us&pip=true";

const HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
  "x-api-key": "yP6bAmceig0QmrXzGfx3IG867h5jKkAs",
  authorization:
    "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NjI5NjM3NDMsImlhdCI6MTc2Mjk2MTk0MywiaW50ZXJuYWxKd3QiOiJVMkZzZEdWa1gxL3dtZnVTcXc3UEU2YStwbVRBb2FtelBpMC96VEdWaHhKKzFuZ0VrK05PQm02OTZpTHExTjhUdC9HNUJtZEpRM01QOTE1Q0VIUWtyRXJ3YU1YT1pUZHhIdndnc1BFWXVvZVZwbDNFR0gvaGlleEJncHdNR05NNVZUS1lvNEtQM1d0UXVyVjZvbTV6M2Q3RFVjL05lQ254dU0wQWdjQ0h0cEdGWTROcEQzMTNVV3FsblI0LzFVS0J0QjJrdDZ6Z1FscUNtSUdybGxGd3dXeXhpblZzaHpXWFVjakQ2UlJwbXdrbDBiWGVIN3RDKzBOaTZicEZDbW5rMk1tL3J1M1NtVlFnQ1ZKalFUeEFJK2dMeXpoaTRDaTVHODVuQ3ZBWURYNzhqOCs3Vm5PN3A1V2VpQXB0MDRrQTg2Sks4dTU1THhMeDZNOUpSUjVxbzZCdVVUeUU2U1hRUGpOeWFwc2gyK2hJMDhzTkdza09EQk1UTGI0Rjl5MGszcWZpb2VkazNZNEdTT1Y3RkZ6TzAzbnpuSFF0QkNEeW5pV1ZtS0xHclJad0VWME40bGVZZi9hQTNteUtTSzl1STdxNTY3WjFJZ1hHWVhXMkwrNVpUaFVvWStoTk1Bczh4S0lSS1ZSTGhtVzVMcGh4M04xNHk1OW5QS1JjZGFyMlBSMGtBbkdsMC9VVlpPMThLT3FhQzByaTJYZUlwL1dPb0hHSzgvTEh1Z2xNU3VjZTZiY3MzYVBGYmQ5a21wdWtuRWF3Rk93KzNNQStncFgrQTN5VzdVTXQrUU9LdFN3UkkxRDFCUXF0bzliT3h6TThoZ21jcGNZRXpWRGc2OVNHRlgzOFlQVmRZa3I1cTZlUWYzaDRrZkRzYmxpUS9BVUpGaEk3b1NBU1k5TEE2eDI2aGJTZUVleTdaNWQ2NHZEM0cvSVlXcXhRZTNERi92LzNQRDV0cmdBMUFTRjhxOUlKa3lNZ3FtQXBCWWNuc3RsdGUzOC8zbUhLZzVhK2YvcUt4QlFDcDF5Q01WSGlJdTNNekJqWWUrVXZxdVJPQWhCOVVWVktpckJuOVhDTkMwVm5KeExyRlQxSjVuZzBBQ0Z0YlFUNFUzNUhleU05NG1MUDVGV3NVcXFWUkpOUEtOek8wOG1sYkZXRVExSlZCNkZlT1pvNnZpbEp4NFR3ZWwwRzFiWk9ZQjdnT3NzbGd3ZUlxb0NZNkpLK0pBSHJiVDRhcXZodDQwTUptV1dOYk5WWjNwWWFhalNFYWRVQ2V1TWt6cCsyekVheWlRZ29Sdmw5YXVyM2QzRm1xYVd0MDd2V1dHSElleVR4bG5aR2lUd0xCdkFhbDN0WktyL0VDRlA2eFdLdGFPM0ZBL29QcGVRZG9oeDdHd29UaVlta013eVZqSndmRXAvZ2w2Z0J2T2sxdUZWdXBFaTlYK2NoZHRRaW1XM1BPTUY4eVBDOG5id1VsdG5Tbm1ucTVjekJBdkV0M1pTenI2YlNvM0RNTWkraDh3NlhVdGlTVE82M0lBWm4wZUlBN28wWk01Z2lMVXZjY1VxbGpGbnlib0JVbnlzQWY3enZ1ZysvV243MzdxR3oxVkd2Wjg4S2VzaWFoQURPaDhzSDYwcTVrNmNCb20zZmE4OGwydnVpOU4xV3lQWjhVVFdqbjRQQ3p4SEMrN2FKUGkxQmZuZUtaQyt5ay9sQng1SEZuZGY3SXRNRWhWQ1ZGTkxDc3c9PSIsImN1c3RvbWVySW5mbyI6eyJjdXN0b21lcklkIjoiYmNabURiUGVmU0xpMVVqU2oxWmFtZXIxbTgiLCJyZWdpc3RlcmVkIjpmYWxzZX0sImNhY2hlUG9saWN5Ijp7ImtleSI6IiIsImxvb2t1cCI6dHJ1ZSwicG9wdWxhdGUiOnRydWV9fQ.qYGmw2vZ3NuYPKoFE2c-67S5SeErr9zDwoqLYC960T0",
  Cookie:
    "optimizelyEndUserId=oeu1762933330945r0.599862886386541; BVBRANDID=020024e6-b217-4f71-baca-f73b862de9a8; dwsiteid=ToryBurch_US; shipping-destination=US; attentivewelcomeoffer=true; fs_interstitial_popup_new_v1-cookie=true; utm_source=google; _gcl_au=1.1.227744042.1762933488; _ga=GA1.1.570620764.1762933491; __kla_id=eyJjaWQiOiJZMkZrWkRneU5UVXRPRE5rTnkwME9UTTNMV0UyT0RrdE5UWmxaR1E1TUdWaE56RTQifQ==; _tt_enable_cookie=1; _ttp=01K9VGCHQ24HS9JY26KF9V3S4T_.tt.1; FPIDTT=FPID2.2.mzZFErrqCbP9p29AJZfZeDP5n2eU1v996rIXNW7PyPM%3D.1762933491; FPLC=p9Wm8IIbcVykUWk7GlKMYTYqoBrVu5PqdC1AHsOmYwSGC1JafVgNMe7QFOjrKCU%2BzxbLJI1ekpM91EDqfBUJNnEdV0aco3P%2F3Jskdn4Vnhfat2djrbexs2mF27Yhsw%3D%3D; FPAU=1.1.227744042.1762933488; _pin_unauth=dWlkPVkyUTNOMlJrTldFdE9UaGlOeTAwT1ROa0xUZ3pNamt0TURZeU5HRTRNamt5TWpFeQ; _scid=0oXbSW_7gnn99OKHtdxv_rKal8vjPxL0; _fbp=fb.1.1762933493398.576976803676865688; _cs_c=0; _lr_geo_location=PK; _ScCbts=%5B%5D; _hjHasCachedUserAttributes=true; __attentive_id=9f7ff4e29c724fcab4cc00c358c0cf7a; _attn_=eyJ1Ijoie1wiY29cIjoxNzYyOTMzNDk0Mzk0LFwidW9cIjoxNzYyOTMzNDk0Mzk0LFwibWFcIjoyMTkwMCxcImluXCI6ZmFsc2UsXCJ2YWxcIjpcIjlmN2ZmNGUyOWM3MjRmY2FiNGNjMDBjMzU4YzBjZjdhXCJ9In0=; __attentive_cco=1762933494397; __attentive_dv=1; _sctr=1%7C1762887600000; optimizelySession=0; xgen_user=%7B%22userId%22%3A%22g3inbfm5w94mhvp2zmn%22%2C%22userType%22%3A%22return_user%22%2C%22exp%22%3A1797061491167%7D; ak_bmsc=5051EC9BB8F3DF3E9142E48EA184CBDF~000000000000000000000000000000~YAAQ1apkX/YXs1yaAQAAiaoLeB0MHKrB1DCyno+xjSg5KH5w/T/20SgRP5mk8m/UN3eE2gavjcC/Qkiko/ZLu5maZJTivlTezuhtjy/nZ5XzpRmnJVKM4qO7h9MG3jRWNgbDatPHhiXWw8x5U1bBM+0QRieqoU5ijd/tACvcS7sANFbNYJYwCB8K2jFJpfgHiajhbTgVUTWXHkXVoohzNRR8/R50ul1V3L/odBDoZKo5xsFozdVuk4nJc94UAUGvPabNlmQa2PXYJpskRrD0u5Ng2iekA5gmlv5SZzRbSADbLtgYalYyJFhYVwCaf6janMXIPXq+h9XaIFhFV3z+SdL1etjHRxkUrqHmwn2DzhIWKM+dnqliafNHVSYkvTihu7c/18SaAvGwvSFYulrS; BVBRANDSID=67acccc0-1aa7-4d2b-b88c-814c85275d60; xgen_session=%7B%22sessionId%22%3A%227a3248a5-26d0-4274-bb62-95f7b689a57c%22%2C%22exp%22%3A1762952491527%7D; _uetsid=fe3256e0bfa211f08e71a9b5e03f7c83; _uetvid=fe339410bfa211f082f18771240d282d; bm_sv=FD574717DC5471103661091E04FE36C7~YAAQlapkX3L/CSaaAQAA9koneB3BVGaDRmuLEFzlDcAi+iZ46wL7eg37RlkjtDSLV6futu0sCVGBr7GWmvU6fXhwfusp6roljnCCem8I8uyEl//6KRf5gxfqGp8oCAJksteKrCK6kX+Alk1q23An/aN08O9w6bQKq1xEUj6yicPmW1K2iWnsnGLWO3FibfSfL570Ucy5Jhq1HEIpNlQDiTj8Rl4rL1rsQ9vUl9C1EhC6lLmGR/CUmQRL2W9AMbwmSriUiw==~1; _abck=9954B98CB0DD4445754BCA7B25540771~0~YAAQ1apkXw55s1yaAQAAYX0neA5ubHjAm/+gxXApE6um2IuWuL5DT+l9GMqIAal97EjBQWdf+Vb3akPhTb0Q9XOCKA4fNqHOQDM1liFzp1SG6VwFVVzVQeaHYMyZOEPIVAT4/k1/Ng1yRjZaomKl7EF4DW+cwWgPr8EDrJPCOCvrmL9m5AsDMTUlltUwZhhiPGUc3MSVtoexxwXQr9S8IJ09flQMB2x9FSCJHAbDf20VcT8f9Y27lBHzeT+hw+Gel+cXMMSz5ykbxMp72vx9LXra+v7hQd9hkFN+dX7L6mxpqVaVFaDvGkiAKmovnnZ716Q5QLXwjVkBUyFluSoa5a1xNWRObuSajb1Ph/PyMrW3V4U+qX94ViTyOM/zvtuRv5Xo7tmJUqfC2wIY5KsLjYXxzU9dMLvd+9VNkYr6CGRqVDDcV5wWkA6lnKqlOCfweoRoGiRHTsJ3TL+Vbhkx32wxzdeMU0FlhgaxCO7K1BgSdtbmSrZAfi8dwWXsKMCJj00iJVygoG+zrZb0fpDl0hNRHRM26USkqXbvuOjBfcSn/EYS23R96OW1gjFuA9fUnurDqChUbIBN/ixT5j3BT3xZlUQ/zO1iiieplccSeuVYZDZyIL5a3s/eHSV4lIkyFpo+Xw==~-1~-1~-1~AAQAAAAE%2f%2f%2f%2f%2f83NLSr5wbR5bkChU2HHbt9dBC5KzCewEHsa3UMBJvTlL4KsCoBbB3PmG9%2f7oXiQVglMFXsGl3XwIKP5tcb5DTaG6N9Idozev6Yr4wttioMCIxKN+Vt3agD9EVWwsm1JMJa8nOc%3d~-1",
  Accept: "application/json",
  "Accept-Language": "en-US,en;q=0.9",
  Referer: "https://www.toryburch.com/en-us/clothing/",
  locale: "en_US",
};

const DETAIL_HEADERS = {
  ...HEADERS,
  Accept:
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
};

const PRODUCT_DETAIL_CACHE = new Map();

function buildProductUrl(product = {}, swatch = {}) {
  const staticPath = product.staticURL || swatch.staticURL || "";

  if (!staticPath) return "";

  if (/^https?:\/\//i.test(staticPath)) {
    const colorNumber = swatch.colorNumber || product.preselectedColor || "";
    const url = new URL(staticPath);
    if (colorNumber && !url.searchParams.has("color")) {
      url.searchParams.set("color", colorNumber);
    }
    return url.toString();
  }

  const cleanedPath = staticPath.replace(/^\//, "");
  const hasLocalePrefix = /^en[-_]us\//i.test(cleanedPath);
  const localePrefixedPath = hasLocalePrefix
    ? cleanedPath
    : `en-us/${cleanedPath}`;

  const productId = product.id || product.styleId || "";

  const pathWithoutTrailingSlash = localePrefixedPath.replace(/\/+$/, "");
  const pathHasHtmlExtension = /\.html?$/i.test(pathWithoutTrailingSlash);

  const pathWithIdentifier =
    productId && !pathHasHtmlExtension
      ? `${pathWithoutTrailingSlash}/${productId}.html`
      : pathWithoutTrailingSlash;

  const baseUrl = `https://www.${STORE_DETAILS.retailer_domain}/${pathWithIdentifier}`;

  const colorNumber = swatch.colorNumber || product.preselectedColor || "";
  if (!colorNumber) return baseUrl;

  const separator = baseUrl.includes("?") ? "&" : "?";
  return `${baseUrl}${separator}color=${encodeURIComponent(colorNumber)}`;
}

function deriveGender(product = {}) {
  const {
    productDepartmentName = "",
    productDepartmentId = "",
    staticURL = "",
  } = product;
  const haystack =
    `${productDepartmentName} ${productDepartmentId} ${staticURL}`.toLowerCase();

  if (haystack.includes("men") && !haystack.includes("women")) return "Male";
  if (haystack.includes("boys")) return "Boy";
  if (haystack.includes("girls")) return "Girl";

  // Default to Female for Tory Burch women's clothing section
  return "Female";
}

function extractMaterials(product = {}) {
  if (product.materials) return product.materials;
  if (product.material) return product.material;
  if (product.attributes && product.attributes.material)
    return product.attributes.material;
  return "";
}

function decodeHtmlEntities(value = "") {
  if (typeof value !== "string" || value.length === 0) return value || "";

  const entityMap = {
    "&amp;": "&",
    "&lt;": "<",
    "&gt;": ">",
    "&quot;": '"',
    "&#39;": "'",
    "&apos;": "'",
    "&nbsp;": " ",
    "&reg;": "®",
    "&trade;": "™",
  };

  return value.replace(/&(?:[a-z]+|#[0-9]+|#x[0-9a-f]+);/gi, (match) => {
    const normalized = match.toLowerCase();
    if (entityMap[normalized]) return entityMap[normalized];
    if (normalized.startsWith("&#x")) {
      const charCode = parseInt(normalized.slice(3, -1), 16);
      if (!Number.isNaN(charCode)) return String.fromCharCode(charCode);
    }
    if (normalized.startsWith("&#")) {
      const charCode = parseInt(normalized.slice(2, -1), 10);
      if (!Number.isNaN(charCode)) return String.fromCharCode(charCode);
    }
    return match;
  });
}

function safeJsonParse(content = "") {
  try {
    return JSON.parse(content);
  } catch (error) {
    return null;
  }
}

function extractJsonLdNodes(html = "") {
  if (!html || typeof html !== "string") return [];

  const scriptRegex =
    /<script\s+type=["']application\/ld\+json["']>([\s\S]*?)<\/script>/gi;
  const matches = Array.from(html.matchAll(scriptRegex));

  return matches
    .map((match) => {
      const rawContent = match[1]?.trim() || "";
      if (!rawContent) return null;
      const cleanedContent = rawContent.replace(/<!--[\s\S]*?-->/g, "").trim();
      return safeJsonParse(cleanedContent);
    })
    .filter(Boolean);
}

function collectProductNodes(source, bucket = []) {
  if (!source) return bucket;

  if (Array.isArray(source)) {
    source.forEach((item) => collectProductNodes(item, bucket));
    return bucket;
  }

  if (typeof source !== "object") return bucket;

  const type = source["@type"];
  const types = Array.isArray(type) ? type : type ? [type] : [];

  if (types.includes("Product") || types.includes("ProductGroup")) {
    bucket.push(source);
  }

  Object.keys(source).forEach((key) => {
    if (key === "@context") return;
    const value = source[key];
    if (value && (typeof value === "object" || Array.isArray(value))) {
      collectProductNodes(value, bucket);
    }
  });

  return bucket;
}

function normalizeIdentifier(value) {
  return String(value || "")
    .trim()
    .toLowerCase();
}

function matchesProductIdFromNode(node = {}, productId = "") {
  const normalizedId = normalizeIdentifier(productId);
  if (!normalizedId) return false;

  const candidates = [
    node.productGroupID,
    node.productId,
    node.productID,
    node.sku,
    node.styleId,
    node.id,
    node["@id"],
  ]
    .filter(Boolean)
    .map((candidate) => normalizeIdentifier(candidate));

  return candidates.some((candidate) => {
    if (!candidate) return false;
    if (candidate === normalizedId) return true;
    if (candidate.endsWith(`/${normalizedId}`)) return true;
    if (candidate.includes(`${normalizedId}.html`)) return true;
    return candidate.includes(normalizedId);
  });
}

function matchesColorFromNode(node = {}, colorNumber = "", colorName = "") {
  const normalizedColorNumber = normalizeIdentifier(colorNumber);
  const normalizedColorName = normalizeIdentifier(colorName).replace(
    /\s+/g,
    ""
  );

  const candidateUrl = normalizeIdentifier(
    node.url || (node.offers && node.offers.url) || ""
  );
  if (
    normalizedColorNumber &&
    candidateUrl.includes(`color=${normalizedColorNumber}`)
  ) {
    return true;
  }

  const colors = node.color
    ? Array.isArray(node.color)
      ? node.color
      : [node.color]
    : [];

  if (
    normalizedColorName &&
    colors.some(
      (value) =>
        normalizeIdentifier(value).replace(/\s+/g, "") === normalizedColorName
    )
  ) {
    return true;
  }

  if (
    normalizedColorNumber &&
    node.sku &&
    normalizeIdentifier(node.sku).includes(normalizedColorNumber)
  ) {
    return true;
  }

  return false;
}

function extractMaterialFromNode(node = {}) {
  if (!node || typeof node !== "object") return "";

  const materialValue = node.material || node.materials;
  if (materialValue) {
    if (Array.isArray(materialValue)) {
      return materialValue
        .map((value) => (typeof value === "string" ? value : ""))
        .filter(Boolean)
        .join(", ");
    }
    if (typeof materialValue === "string") {
      return materialValue;
    }
  }

  const additionalProperties =
    node.additionalProperty || node.additionalProperties;
  if (Array.isArray(additionalProperties)) {
    const materialProperty = additionalProperties.find((property = {}) => {
      const name = normalizeIdentifier(property.name);
      return name === "material" || name === "materials";
    });
    if (materialProperty?.value) {
      return Array.isArray(materialProperty.value)
        ? materialProperty.value.join(", ")
        : materialProperty.value;
    }
  }

  if (Array.isArray(node.hasVariant)) {
    for (const variant of node.hasVariant) {
      const material = extractMaterialFromNode(variant);
      if (material) return material;
    }
  }

  return "";
}

function toNumber(value, fallback = 0) {
  if (value === null || value === undefined) return fallback;
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function selectProductDetailFromNodes(nodes = [], product = {}, swatch = {}) {
  if (!Array.isArray(nodes) || nodes.length === 0) return {};

  const collectedNodes = [];
  nodes.forEach((node) => collectProductNodes(node, collectedNodes));

  if (!collectedNodes.length) return {};

  const productId = product.id || product.styleId || product.parent_product_id;
  const colorNumber = swatch.colorNumber || product.preselectedColor || "";
  const colorName = swatch.colorName || swatch.colorGroup || "";

  const isProductGroup = (node) => {
    const types = Array.isArray(node["@type"])
      ? node["@type"]
      : node["@type"]
      ? [node["@type"]]
      : [];
    return types.includes("ProductGroup");
  };

  const isProduct = (node) => {
    const types = Array.isArray(node["@type"])
      ? node["@type"]
      : node["@type"]
      ? [node["@type"]]
      : [];
    return types.includes("Product");
  };

  const matchingGroup = collectedNodes.find(
    (node) => isProductGroup(node) && matchesProductIdFromNode(node, productId)
  );

  const fallbackGroup = collectedNodes.find((node) => isProductGroup(node));

  const variantNode = collectedNodes.find(
    (node) =>
      isProduct(node) &&
      (matchesProductIdFromNode(node, productId) ||
        matchesColorFromNode(node, colorNumber, colorName))
  );

  const fallbackProduct = collectedNodes.find((node) => isProduct(node));

  const primaryNode =
    variantNode || matchingGroup || fallbackGroup || fallbackProduct || {};
  const secondaryNode =
    primaryNode !== matchingGroup && matchingGroup
      ? matchingGroup
      : variantNode || fallbackProduct || {};

  const description =
    primaryNode.description ||
    secondaryNode.description ||
    product.shortDescription ||
    product.longDescription ||
    product.description ||
    "";

  const materials =
    extractMaterialFromNode(primaryNode) ||
    extractMaterialFromNode(secondaryNode) ||
    "";

  const aggregateRating =
    primaryNode.aggregateRating ||
    secondaryNode.aggregateRating ||
    matchingGroup?.aggregateRating ||
    fallbackProduct?.aggregateRating ||
    {};

  const averageRating = toNumber(
    aggregateRating?.ratingValue ?? aggregateRating?.rating
  );
  const reviewCount = toNumber(
    aggregateRating?.reviewCount ?? aggregateRating?.ratingCount
  );
  const ratingCount = toNumber(
    aggregateRating?.ratingCount ?? aggregateRating?.reviewCount
  );

  return {
    description: decodeHtmlEntities(description),
    materials: decodeHtmlEntities(materials),
    averageRating,
    reviewCount,
    ratingsCount: ratingCount || reviewCount,
  };
}

function extractSizeAvailability(html = "") {
  if (!html || typeof html !== "string") return {};

  const sizeAvailabilityMap = {};

  // Match all size option elements
  const optionRegex =
    /<option[^>]*value=["']([^"']+)["'][^>]*>([\s\S]*?)<\/option>/gi;
  const matches = Array.from(html.matchAll(optionRegex));

  matches.forEach((match) => {
    const fullTag = match[0];
    const sizeValue = match[1];
    const content = match[2];

    // Skip empty or "Select Size" options
    if (!sizeValue || sizeValue === "" || content.includes("Select Size")) {
      return;
    }

    // Check if the option has disabled attribute
    const isDisabled = /\bdisabled\b/i.test(fullTag);
    const isSoldOut =
      isDisabled ||
      /sold\s*out/i.test(content) ||
      /out\s*of\s*stock/i.test(content);

    sizeAvailabilityMap[sizeValue.trim()] = !isSoldOut;
  });

  return sizeAvailabilityMap;
}

function extractMaterialsFromHtml(html = "") {
  if (!html || typeof html !== "string") return "";

  const materials = [];

  // Match all <li class="desc-list__item-_oQ"> elements
  const listItemRegex =
    /<li\s+class=["']desc-list__item-_oQ["']>([\s\S]*?)<\/li>/gi;
  const matches = Array.from(html.matchAll(listItemRegex));

  matches.forEach((match) => {
    let content = match[1];

    // Remove HTML tags from content
    content = content.replace(/<[^>]*>/g, "");
    // Decode HTML entities
    content = decodeHtmlEntities(content);
    // Clean up whitespace
    content = content.replace(/&nbsp;/g, " ").trim();

    // Check if this line contains material information (percentages like "57% silk")
    const hasMaterialPercentage = /\d+%\s*[a-zA-Z]/i.test(content);
    const hasCertifiedMaterial = /certified|responsibly|sustainable/i.test(
      content
    );
    const materialKeywords =
      /\b(silk|cotton|wool|polyester|viscose|nylon|linen|cashmere|leather|suede|denim|spandex|elastane|rayon|acrylic|modal)\b/i;

    // Only include if it has percentage notation or certified material keywords
    if (
      hasMaterialPercentage ||
      (hasCertifiedMaterial && materialKeywords.test(content))
    ) {
      materials.push(content);
    }
  });

  // Return unique materials joined by "; "
  const uniqueMaterials = Array.from(new Set(materials));
  return uniqueMaterials.join("; ");
}

function parseProductDetailFromHtml(html = "", product = {}, swatch = {}) {
  if (!html || typeof html !== "string") return {};
  const nodes = extractJsonLdNodes(html);
  const jsonLdData = nodes.length
    ? selectProductDetailFromNodes(nodes, product, swatch)
    : {};

  // Extract size availability from HTML
  const sizeAvailability = extractSizeAvailability(html);

  // Extract materials from HTML
  const htmlMaterials = extractMaterialsFromHtml(html);

  return {
    ...jsonLdData,
    sizeAvailability,
    // Prefer HTML materials over JSON-LD materials if available
    materials: htmlMaterials || jsonLdData.materials || "",
  };
}

async function fetchProductDetail(product = {}, swatch = {}) {
  const candidateUrl =
    buildProductUrl(product, swatch) || buildProductUrl(product, {});

  if (!candidateUrl) return {};

  const cacheKey = product.id
    ? `${product.id}-${swatch.colorNumber || "default"}`
    : candidateUrl;

  if (PRODUCT_DETAIL_CACHE.has(cacheKey)) {
    return PRODUCT_DETAIL_CACHE.get(cacheKey);
  }

  try {
    const response = await fetch(candidateUrl, { headers: DETAIL_HEADERS });
    if (!response.ok) {
      throw new Error(`Detail request failed with status ${response.status}`);
    }

    const html = await response.text();
    const detail = parseProductDetailFromHtml(html, product, swatch);

    PRODUCT_DETAIL_CACHE.set(cacheKey, detail);
    if (product.id && cacheKey !== `${product.id}-default`) {
      PRODUCT_DETAIL_CACHE.set(`${product.id}-default`, detail);
    }

    return detail;
  } catch (error) {
    console.warn(
      `Failed to fetch detail for product ${product.id || ""}:`,
      error.message
    );
    const fallback = {};
    PRODUCT_DETAIL_CACHE.set(cacheKey, fallback);
    if (product.id && cacheKey !== `${product.id}-default`) {
      PRODUCT_DETAIL_CACHE.set(`${product.id}-default`, fallback);
    }
    return fallback;
  }
}

function normalizeBadge(badge = {}) {
  if (!badge || typeof badge !== "object") return {};
  return {
    id: badge.id || "",
    message: badge.message || "",
    styleId: badge.styleId || "",
  };
}

function calculatePriceDetails(price = {}, fallbackPrice = {}) {
  const source = price && Object.keys(price).length ? price : fallbackPrice;
  const currency = source.currency || fallbackPrice.currency || "USD";
  const min = Number(source.min ?? fallbackPrice.min ?? 0);
  const max = Number(source.max ?? fallbackPrice.max ?? min);
  const type = (source.type || fallbackPrice.type || "").toLowerCase();

  const originalPrice = max || min;
  const finalPrice = min || max || 0;
  const isOnSale =
    type.includes("sale") || (finalPrice > 0 && originalPrice > finalPrice);
  const salePrice = isOnSale ? finalPrice : 0;

  return {
    price_currency: currency,
    original_price: originalPrice,
    final_price: finalPrice,
    sale_price: salePrice,
    is_on_sale: isOnSale,
  };
}

function buildVariant(
  product,
  swatch = {},
  size = {},
  sizeAvailabilityMap = {}
) {
  const priceDetails = calculatePriceDetails(swatch.price, product.price || {});

  const sizeValue = size.value || size.name || "";
  const sizeName = size.name || size.label || sizeValue;
  const colorName = swatch.colorName || swatch.colorGroup || "";
  const colorNumber = swatch.colorNumber || "";
  const originalPrice = priceDetails.original_price;
  const finalPrice = priceDetails.final_price;
  const salePrice = priceDetails.sale_price;
  const isOnSale = priceDetails.is_on_sale;
  const discount =
    isOnSale && originalPrice
      ? Math.round(((originalPrice - finalPrice) / originalPrice) * 100)
      : 0;

  const buildImageUrl = (code) => {
    if (!code) return "";
    if (code.includes("/")) {
      return `https://s7.toryburch.com/is/image/ToryBurch/${code}.pdp-1919x2180.jpg`;
    }
    return `https://s7.toryburch.com/is/image/ToryBurch/style/${code}.pdp-1919x2180.jpg`;
  };

  const imageCodes = [];
  if (swatch.imageCode) imageCodes.push(`${swatch.imageCode}_SLFRO`);
  if (Array.isArray(swatch.images)) imageCodes.push(...swatch.images);

  const uniqueCodes = Array.from(new Set(imageCodes.filter(Boolean)));
  const alternateImages = uniqueCodes.map(buildImageUrl).filter(Boolean);
  const imageUrl = alternateImages[0] || buildImageUrl(swatch.imageCode);
  const sellingPrice = isOnSale ? finalPrice : originalPrice;
  const salePriceValue = isOnSale ? finalPrice : 0;

  const productUrl = buildProductUrl(product, swatch);

  // Determine stock status from HTML-extracted data first, then fallback to API data
  let isInStock = true;
  if (sizeValue && Object.keys(sizeAvailabilityMap).length > 0) {
    // Try exact match first
    if (sizeAvailabilityMap.hasOwnProperty(sizeValue)) {
      isInStock = sizeAvailabilityMap[sizeValue];
    } else {
      // Try case-insensitive match
      const normalizedSize = sizeValue.toLowerCase().trim();
      const matchingKey = Object.keys(sizeAvailabilityMap).find(
        (key) => key.toLowerCase().trim() === normalizedSize
      );
      if (matchingKey) {
        isInStock = sizeAvailabilityMap[matchingKey];
      } else {
        // Fallback to API data
        isInStock =
          size.isAvailable ??
          size.available ??
          size.in_stock ??
          size.inStock ??
          true;
      }
    }
  } else {
    // No HTML data available, use API data
    isInStock =
      size.isAvailable ??
      size.available ??
      size.in_stock ??
      size.inStock ??
      true;
  }

  return {
    price_currency: priceDetails.price_currency,
    original_price: originalPrice,
    link_url: productUrl,
    deeplink_url: productUrl,
    image_url: imageUrl,
    alternate_image_urls: alternateImages,
    is_on_sale: isOnSale,
    is_in_stock: isInStock,
    size: sizeValue,
    size_label: sizeName,
    color: colorName,
    mpn: `${product.id}-${colorName}`,
    ratings_count: toNumber(product.ratingsCount ?? product.reviewCount ?? 0),
    average_ratings: toNumber(product.averageRating ?? 0),
    review_count: toNumber(product.reviewCount ?? product.ratingsCount ?? 0),
    selling_price: sellingPrice,
    sale_price: salePriceValue,
    final_price: finalPrice,
    discount,
    operation_type: "INSERT",
    variant_id: uuidv5(
      `${product.id}-${colorNumber}-${colorName}-${sizeValue}`,
      VARIANT_NAMESPACE
    ),
    variant_description: "",
  };
}

async function transformProduct(product = {}) {
  const sizes = Array.isArray(product.sizes) ? product.sizes : [];
  const swatches = Array.isArray(product.swatches) ? product.swatches : [];

  const primarySwatch =
    swatches.find(
      (swatch) =>
        swatch &&
        swatch.colorNumber &&
        swatch.colorNumber === product.preselectedColor
    ) ||
    swatches[0] ||
    {};

  const detail = await fetchProductDetail(product, primarySwatch);

  const averageRating = toNumber(
    detail.averageRating ?? product.averageRating ?? 0
  );
  const reviewCount = toNumber(detail.reviewCount ?? product.reviewCount ?? 0);
  const ratingsCount = toNumber(
    detail.ratingsCount ?? product.ratingsCount ?? reviewCount
  );

  const enrichedProduct = {
    ...product,
    averageRating,
    reviewCount,
    ratingsCount,
  };

  // Get size availability map from HTML
  const sizeAvailabilityMap = detail.sizeAvailability || {};

  const variants = [];

  if (swatches.length && sizes.length) {
    swatches.forEach((swatch) => {
      sizes.forEach((size) => {
        variants.push(
          buildVariant(enrichedProduct, swatch, size, sizeAvailabilityMap)
        );
      });
    });
  } else if (swatches.length) {
    swatches.forEach((swatch) => {
      variants.push(
        buildVariant(enrichedProduct, swatch, {}, sizeAvailabilityMap)
      );
    });
  } else if (sizes.length) {
    sizes.forEach((size) => {
      variants.push(
        buildVariant(enrichedProduct, {}, size, sizeAvailabilityMap)
      );
    });
  } else {
    variants.push(buildVariant(enrichedProduct, {}, {}, sizeAvailabilityMap));
  }

  const description =
    detail.description ||
    product.shortDescription ||
    product.longDescription ||
    product.description ||
    "";

  const category =
    product.productClassName ||
    product.productDepartmentName ||
    product.primaryCategoryId ||
    "Apparel";

  return {
    parent_product_id: product.id,
    name: product.name,
    description,
    category,
    brand: product.brand || STORE_DETAILS.brand,
    retailer_domain: STORE_DETAILS.retailer_domain,
    gender: deriveGender(product),
    materials: detail.materials || extractMaterials(product),
    return_policy_link: STORE_DETAILS.return_policy_link,
    return_policy: STORE_DETAILS.return_policy,
    size_chart: STORE_DETAILS.size_chart,
    available_bank_offers: "",
    available_coupons: "",
    variants,
    operation_type: "INSERT",
    source: "toryburch",
  };
}

async function fetchCategoryProducts(endpoint) {
  const response = await fetch(endpoint, { headers: HEADERS });
  if (!response.ok) {
    throw new Error(`Request failed with status ${response.status}`);
  }
  return response.json();
}

async function fetchCategoryWithPagination(categorySlug, limit = 100) {
  let offset = 0;
  let allProducts = [];
  let hasMore = true;

  console.log(`\n📦 Fetching category: ${categorySlug}`);

  while (hasMore) {
    try {
      const endpoint = `${API_BASE_URL}/${categorySlug}/products?${API_PARAMS}&limit=${limit}&offset=${offset}`;
      const data = await fetchCategoryProducts(endpoint);

      if (data && Array.isArray(data.products) && data.products.length > 0) {
        allProducts.push(...data.products);
        console.log(
          `   ✓ Fetched ${data.products.length} products (offset: ${offset}, total: ${allProducts.length})`
        );

        // Check if there are more products
        const totalProducts = data.total || data.productCount || 0;
        if (offset + limit >= totalProducts || data.products.length < limit) {
          hasMore = false;
        } else {
          offset += limit;
        }

        // Small delay to avoid rate limiting
        await new Promise((resolve) => setTimeout(resolve, 500));
      } else {
        hasMore = false;
      }
    } catch (error) {
      console.warn(
        `   ⚠ Error fetching ${categorySlug} at offset ${offset}:`,
        error.message
      );
      hasMore = false;
    }
  }

  console.log(`   ✅ Category ${categorySlug}: ${allProducts.length} products`);
  return allProducts;
}

async function fetchAllCategories() {
  const allProducts = [];
  const productIds = new Set();

  console.log(
    `\n🚀 Starting to fetch ${CLOTHING_CATEGORIES.length} categories...`
  );

  for (const category of CLOTHING_CATEGORIES) {
    const products = await fetchCategoryWithPagination(category);

    // Deduplicate products by ID
    const newProducts = products.filter((product) => {
      if (product.id && !productIds.has(product.id)) {
        productIds.add(product.id);
        return true;
      }
      return false;
    });

    if (newProducts.length > 0) {
      allProducts.push(...newProducts);
    }

    console.log(
      `   📊 Progress: ${allProducts.length} unique products collected so far`
    );
  }

  console.log(`\n✨ Total unique products found: ${allProducts.length}`);
  return allProducts;
}

function ensureOutputDirectory() {
  const brandSlug = STORE_DETAILS.brand.toLowerCase().replace(/\s+/g, "");
  const outputDir = path.join(
    __dirname,
    "output",
    STORE_DETAILS.country,
    `${brandSlug}-${STORE_DETAILS.country}`
  );

  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }

  return outputDir;
}

function buildStoreInfo(products = []) {
  const categories = Array.from(
    new Set(
      products
        .map(
          (product) =>
            product.category ||
            product.product_class_name ||
            product.product_department_name
        )
        .filter((value) => value && value.trim() !== "")
    )
  );

  return {
    name: STORE_DETAILS.brand,
    domain: STORE_DETAILS.retailer_domain,
    currency: STORE_DETAILS.currency,
    country: STORE_DETAILS.country,
    total_products: products.length,
    categories: categories.length > 0 ? categories : ["Apparel"],
    crawled_at: new Date().toISOString(),
  };
}

function writeOutputFiles(payload) {
  const outputDir = ensureOutputDirectory();
  const jsonFilePath = path.join(outputDir, "catalog.json");
  const jsonlFilePath = path.join(outputDir, "catalog.jsonl");
  const gzFilePath = `${jsonlFilePath}.gz`;

  fs.writeFileSync(jsonFilePath, JSON.stringify(payload, null, 2), "utf8");

  const jsonlContent = payload.products
    .map((product) => JSON.stringify(product))
    .join("\n");
  fs.writeFileSync(jsonlFilePath, jsonlContent, "utf8");

  const gzippedBuffer = zlib.gzipSync(Buffer.from(jsonlContent, "utf8"));
  fs.writeFileSync(gzFilePath, gzippedBuffer);

  return { jsonFilePath, jsonlFilePath, gzFilePath };
}

async function main() {
  try {
    console.log("🎯 Tory Burch Women's Clothing Scraper");
    console.log("=====================================\n");

    const allProducts = await fetchAllCategories();

    if (!allProducts || allProducts.length === 0) {
      console.log("❌ No products found");
      return { status: "Error", message: "No products found" };
    }

    console.log(`\n🔄 Transforming ${allProducts.length} products...`);
    console.log("   (Fetching product details, stock info, and materials)\n");

    // Transform products in batches to show progress
    const transformedProducts = [];
    const batchSize = 10;

    for (let i = 0; i < allProducts.length; i += batchSize) {
      const batch = allProducts.slice(i, i + batchSize);
      const batchResults = await Promise.all(
        batch.map((product) => transformProduct(product))
      );
      transformedProducts.push(...batchResults);

      console.log(
        `   ✓ Processed ${Math.min(i + batchSize, allProducts.length)}/${
          allProducts.length
        } products`
      );
    }

    const storeInfo = buildStoreInfo(transformedProducts);

    const payload = {
      store_info: storeInfo,
      products: transformedProducts,
    };

    console.log("\n💾 Writing output files...");
    const files = writeOutputFiles(payload);

    console.log("\n🎉 Tory Burch scraping completed successfully!");
    console.log("=====================================");
    console.log(`📊 Total Products: ${transformedProducts.length}`);
    console.log(`📂 Categories: ${storeInfo.categories.length}`);
    console.log(`\n📁 Output Files:`);
    console.log(`   JSON:   ${files.jsonFilePath}`);
    console.log(`   JSONL:  ${files.jsonlFilePath}`);
    console.log(`   GZip:   ${files.gzFilePath}`);

    return payload;
  } catch (error) {
    console.error("❌ Error during scraping:", error);
    return { status: "Error", message: error.message };
  }
}

if (require.main === module) {
  main()
    .then(() => process.exit(0))
    .catch((error) => {
      console.error("Script failed:", error);
      process.exit(1);
    });
}

module.exports = { main };
