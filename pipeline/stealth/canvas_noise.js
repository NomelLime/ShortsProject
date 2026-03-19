// canvas_noise.js — Добавляет субпиксельный шум к Canvas output
// Параметр __CANVAS_SEED__ подставляется из Python перед применением.
//
// Цель: сделать canvas fingerprint уникальным per account.
// Разные seeds → разные hash'и toDataURL() → неотслеживаемые аккаунты.
(function () {
    'use strict';

    const SEED = __CANVAS_SEED__;

    // Быстрый детерминированный PRNG (mulberry32)
    function mulberry32(a) {
        return function () {
            a |= 0;
            a = (a + 0x6D2B79F5) | 0;
            let t = Math.imul(a ^ (a >>> 15), 1 | a);
            t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
            return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
        };
    }

    const rng = mulberry32(SEED);

    function applyNoise(canvas) {
        const ctx = canvas.getContext('2d');
        if (!ctx) return;
        try {
            const imageData = ctx.getImageData(0, 0, canvas.width, canvas.height);
            const data = imageData.data;
            // Добавляем шум к ~2% пикселей (каждый 50-й)
            for (let i = 0; i < data.length; i += 4 * 50) {
                const delta = Math.floor((rng() - 0.5) * 3);
                data[i] = Math.max(0, Math.min(255, data[i] + delta));
            }
            ctx.putImageData(imageData, 0, 0);
        } catch (_) {
            // SecurityError при cross-origin canvas — игнорируем
        }
    }

    // Hook HTMLCanvasElement.prototype.toDataURL
    const origToDataURL = HTMLCanvasElement.prototype.toDataURL;
    HTMLCanvasElement.prototype.toDataURL = function () {
        applyNoise(this);
        return origToDataURL.apply(this, arguments);
    };

    // Hook HTMLCanvasElement.prototype.toBlob
    const origToBlob = HTMLCanvasElement.prototype.toBlob;
    HTMLCanvasElement.prototype.toBlob = function () {
        applyNoise(this);
        return origToBlob.apply(this, arguments);
    };

    // Hook CanvasRenderingContext2D.prototype.getImageData
    const origGetImageData = CanvasRenderingContext2D.prototype.getImageData;
    CanvasRenderingContext2D.prototype.getImageData = function (sx, sy, sw, sh) {
        const imageData = origGetImageData.call(this, sx, sy, sw, sh);
        const data = imageData.data;
        for (let i = 0; i < data.length; i += 4 * 100) {
            const delta = Math.floor((rng() - 0.5) * 2);
            data[i] = Math.max(0, Math.min(255, data[i] + delta));
        }
        return imageData;
    };
})();
