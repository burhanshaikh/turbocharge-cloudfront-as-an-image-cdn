var SUPPORTED_FORMATS = ['auto', 'jpeg', 'webp', 'avif', 'png', 'svg', 'gif'];
var MAX_DIMENSION = 4000; // Example: set a max for width/height for protection

function parsePositiveInt(value, max) {
    // Try to convert the value to an integer
    var parsed = parseInt(value, 10);

    // If parsed is not a number or is not positive, return null
    if (isNaN(parsed) || parsed <= 0) {
        return null;
    }

    // If a max is provided, return the smaller of parsed and max
    if (typeof max === 'number') {
        return Math.min(parsed, max);
    }

    // Otherwise, return the parsed value
    return parsed;
}

function getBestFormatFromAcceptHeader(headers) {
    // Default to jpeg if headers are missing or malformed
    if (typeof headers !== 'object' || !headers['accept'] || !headers['accept'].value) {
        return 'jpeg';
    }

    var acceptHeaderValue = headers['accept'].value;

    // Set serial wise order of priority to match the format from the Accept Header
    if (acceptHeaderValue.indexOf('image/webp') !== -1) {
        return 'webp';
    }
    // Add any other format that you want to convert to
    // if (acceptHeaderValue.indexOf('image/avif') !== -1) {
    //     return 'avif';
    // }
    return 'jpeg';
}

function handler(event) {
    var request = event.request;
    var originalImagePath = request.uri;
    var normalizedOperations = {};
    var formatFromQuery = null;

    if (request.querystring) {
        for (var operation in request.querystring) {
            if (request.querystring.hasOwnProperty(operation)) {
                var value = request.querystring[operation]['value'];
                switch (operation.toLowerCase()) {
                    case 'format':
                        if (value && SUPPORTED_FORMATS.indexOf(value.toLowerCase()) !== -1) {
                            formatFromQuery = value.toLowerCase();
                            if (formatFromQuery === 'auto') {
                                formatFromQuery = null; // Defer to Accept header
                            }
                        }
                        break;
                    case 'width': {
                        var width = parsePositiveInt(value, MAX_DIMENSION);
                        if (width) normalizedOperations['width'] = width.toString();
                        break;
                    }
                    case 'height': {
                        var height = parsePositiveInt(value, MAX_DIMENSION);
                        if (height) normalizedOperations['height'] = height.toString();
                        break;
                    }
                    case 'quality': {
                        var quality = parsePositiveInt(value, 100);
                        if (quality) normalizedOperations['quality'] = quality.toString();
                        break;
                    }
                    default:
                        break;
                }
            }
        }
    }

    // Determine format: query param takes precedence, otherwise use Accept header
    var finalFormat = formatFromQuery || getBestFormatFromAcceptHeader(request.headers);
    normalizedOperations['format'] = finalFormat;

    // Build normalized path with proper URL encoding
    var normalizedOperationsArray = [];
    if (normalizedOperations.format) normalizedOperationsArray.push('format=' + encodeURIComponent(normalizedOperations.format));
    if (normalizedOperations.quality) normalizedOperationsArray.push('quality=' + encodeURIComponent(normalizedOperations.quality));
    if (normalizedOperations.width) normalizedOperationsArray.push('width=' + encodeURIComponent(normalizedOperations.width));
    if (normalizedOperations.height) normalizedOperationsArray.push('height=' + encodeURIComponent(normalizedOperations.height));

    // URL encode the entire operations string to ensure proper path encoding
    request.uri = originalImagePath + '/' + encodeURIComponent(normalizedOperationsArray.join(','));

    // Remove query strings
    request.querystring = {};
    return request;
}
