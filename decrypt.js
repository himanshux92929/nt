/**
 * decrypt_test.js
 *
 * Quick utility to test-decrypt "content-details" style AES-128-CBC
 * encrypted payloads using the hardcoded key/IV found in the client bundle.
 *
 * Usage:
 *   node decrypt_test.js "<base64_ciphertext>"
 *
 * If no argument is passed, it falls back to the SAMPLE value below
 * so you can quickly sanity-check the script still works.
 */

const crypto = require('crypto');

// --- Config: key/IV pulled from the client bundle ---
const KEY = Buffer.from('Ch@tS3cr3tK3y!16'); // 16 bytes -> AES-128
const IV  = Buffer.from('Ch@tIV#16Bytes!!'); // 16 bytes

// Optional: a known-good sample so you can verify the script works
// even without passing an argument.
const SAMPLE = 'oGjN6hGaPPR9yhYWT4IN6V5YLK8jsT+R9zLxlR+YIRhu56e3ClRvfZk6KoBf3k5Rwdl/GKmeyWI1JR+qJcWH50V9xBhan1Yy8arc2w68YLc5jX0LwXiiw64dNAjSVynHHxX+SJxqqBQRfM2EybWvp1JhHvCtOM1hwGiMhIH2NxNriIM7zdBjqT0bgCWpAFnZ6DFguGWap/uHwWO+0fjhGIRhiZ3blp/8XLl63ONKeYdKSCjI2AIB/BqYD5cS/JMlZvo+JbA9F7y2Ngp6pAl8U49vu6DT62bVMvaSCsiuNq+2hxig3z3SlNYqb3FpRHaZ/SPdOOIPlwkIxPHZ2WbCMGpnfSTQVjbuFOpEHmySDiad761mYFQrJX8EM6CFiym+1JOUvodHB3cPYDLQ2jjiqJsGVO4Fewzu3K0lw7xTy2wRIUEE7zgsmiGLTttNsQXv1R7/0JCl9eoK4qvbJFJOI6U0XUeczrHET3Tv/2jNj4CLxGZAjbO5tsHGvO7EVnmRPEZOmaOG3dCo4rmS3/XA2eHRSol+JkW35d1NTeZRMVDsvURVZPKsqCOKufxC5GBPQ14RaR7rUW0n7hp6pCo83JIP/hYczszYsVPkihi64BuzHMBTOhayRU+SKIGlppnez2RjDDz4wVxUV8pEnbm98mWHRmqh+joYy8VcUETAIFD06nshO0AYVXzPsTK9HT8aALl/39RErTBH9NIbE0EZa10QvmzeX4wAznckv3lnYXpw0uhAsKNT+w+jCnsJR985gVy61yLp9Tns8t/Tl0AjRSfTQ+HmcG/VD1G5r8OhsIdD2QXxIBcYoLnpf1zKeosW2xPW30ChoWRMDOgiz5DlPhdw6lTGbrMqtmcFJ4Atg2X5qBnBxfuJIaR+0iJw9PGZWftTPJAvXe/1D4dTGNdC8b3mUjWNKtqOIj+yPtw8asQhXUN37hm0vw/uWjf0BiQS';

/**
 * Decrypts a base64-encoded AES-128-CBC ciphertext string.
 * Returns the parsed JSON object, or throws on failure.
 */
function decryptContentDetails(base64Ciphertext) {
  const decipher = crypto.createDecipheriv('aes-128-cbc', KEY, IV);
  const decrypted = Buffer.concat([
    decipher.update(Buffer.from(base64Ciphertext, 'base64')),
    decipher.final(),
  ]);
  const text = decrypted.toString('utf8');
  return JSON.parse(text);
}

// --- Run ---
const input = process.argv[2] || SAMPLE;

if (!process.argv[2]) {
  console.log('(No argument passed — using built-in SAMPLE for a sanity check)\n');
}

try {
  const result = decryptContentDetails(input);
  console.log('✅ Decryption succeeded:\n');
  console.log(JSON.stringify(result, null, 2));
} catch (err) {
  console.error('❌ Decryption failed:', err.message);
  console.error('\nPossible causes:');
  console.error('  - The string isn\'t valid base64 / was truncated when copy-pasted');
  console.error('  - The key/IV don\'t match this particular payload (e.g. different endpoint or rotated key)');
  console.error('  - The payload wasn\'t actually encrypted with AES-128-CBC');
    }
