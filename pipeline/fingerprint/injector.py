"""
pipeline/fingerprint/injector.py — Применение fingerprint через JS-инъекции.

Принимает fingerprint-профиль (из generator.py) и применяет его
к BrowserContext через add_init_script().

Инъекции выполняются ДО загрузки любой страницы (init_script),
поэтому платформа получает подменённые значения с первого запроса.

Экспортирует:
    apply_fingerprint(context, fp: dict) → None
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict


def _safe_js_string(val) -> str:
    """
    Экранирует значение для безопасной вставки в JavaScript код.

    json.dumps() добавляет кавычки и экранирует все спецсимволы:
    одинарные/двойные кавычки, обратный слэш, переводы строк, Unicode.
    Предотвращает JS injection при вставке fingerprint-полей в init_script.

    Пример:
        _safe_js_string("Win32")         → '"Win32"'
        _safe_js_string("'); alert(1)")  → '"\'); alert(1)"'
    """
    return json.dumps(str(val))

logger = logging.getLogger(__name__)

_STEALTH_DIR = Path(__file__).parent.parent / "stealth"


def apply_fingerprint(context, fp: Dict) -> None:
    """
    Применяет все JS-инъекции fingerprint к BrowserContext.

    Порядок: navigator → screen → canvas → webgl → audio → fonts.
    Вызывать ПОСЛЕ создания контекста, ДО открытия страниц.

    Args:
        context: playwright BrowserContext
        fp:      fingerprint dict от generate_fingerprint()
    """
    _inject_navigator(context, fp)
    _inject_screen(context, fp)
    _inject_canvas(context, fp)
    _inject_webgl(context, fp)
    _inject_audio(context, fp)
    _inject_fonts(context, fp)
    logger.debug(
        "[Fingerprint] Применён для %s (%s, %s, touch=%d)",
        fp.get("device_name", "?"),
        fp.get("user_agent", "")[:50],
        fp.get("timezone_id", "?"),
        fp.get("max_touch_points", 0),
    )


def _inject_navigator(context, fp: Dict) -> None:
    """Подменяет navigator.hardwareConcurrency, deviceMemory, platform, languages и т.д."""
    langs_json = json.dumps(fp["languages"])
    dnt_line = ""
    if fp.get("do_not_track"):
        dnt_val  = _safe_js_string(fp["do_not_track"])
        dnt_line = f"Object.defineProperty(navigator, 'doNotTrack', {{ get: () => {dnt_val} }});"

    context.add_init_script(f"""
    (() => {{
        const _def = (obj, prop, val) => {{
            try {{
                Object.defineProperty(obj, prop, {{ get: () => val, configurable: true }});
            }} catch(_) {{}}
        }};
        _def(navigator, 'hardwareConcurrency', {fp['hardware_concurrency']});
        _def(navigator, 'deviceMemory',        {fp['device_memory']});
        _def(navigator, 'maxTouchPoints',      {fp['max_touch_points']});
        _def(navigator, 'platform',            {_safe_js_string(fp['platform_nav'])});
        _def(navigator, 'languages',           {langs_json});
        _def(navigator, 'language',            {_safe_js_string(fp['languages'][0])});
        {dnt_line}
    }})();
    """)


def _inject_screen(context, fp: Dict) -> None:
    """Подменяет screen.width/height, colorDepth, devicePixelRatio."""
    context.add_init_script(f"""
    (() => {{
        const _def = (obj, prop, val) => {{
            try {{
                Object.defineProperty(obj, prop, {{ get: () => val, configurable: true }});
            }} catch(_) {{}}
        }};
        _def(screen, 'width',      {fp['screen']['width']});
        _def(screen, 'height',     {fp['screen']['height']});
        _def(screen, 'colorDepth', {fp['color_depth']});
        _def(screen, 'pixelDepth', {fp['color_depth']});
        _def(window, 'devicePixelRatio', {fp['pixel_ratio']});
    }})();
    """)


def _inject_canvas(context, fp: Dict) -> None:
    """Добавляет Canvas noise injection из pipeline/stealth/canvas_noise.js."""
    canvas_js_path = _STEALTH_DIR / "canvas_noise.js"
    if not canvas_js_path.exists():
        logger.warning("[Fingerprint] canvas_noise.js не найден: %s", canvas_js_path)
        return
    canvas_js = canvas_js_path.read_text(encoding="utf-8")
    canvas_js = canvas_js.replace("__CANVAS_SEED__", str(fp["canvas_noise_seed"]))
    context.add_init_script(canvas_js)


def _inject_webgl(context, fp: Dict) -> None:
    """Подменяет WebGL vendor и renderer (WebGL1 + WebGL2)."""
    v  = _safe_js_string(fp["webgl_vendor"])
    r  = _safe_js_string(fp["webgl_renderer"])
    uv = _safe_js_string(fp["webgl_unmasked_vendor"])
    ur = _safe_js_string(fp["webgl_unmasked_renderer"])

    context.add_init_script(f"""
    (() => {{
        function patchWebGL(ctor) {{
            if (typeof ctor === 'undefined') return;
            const orig = ctor.prototype.getParameter;
            ctor.prototype.getParameter = function(param) {{
                if (param === 0x1F00) return {v};
                if (param === 0x1F01) return {r};
                if (param === 0x9245) return {uv};
                if (param === 0x9246) return {ur};
                return orig.call(this, param);
            }};
        }}
        patchWebGL(window.WebGLRenderingContext);
        patchWebGL(window.WebGL2RenderingContext);
    }})();
    """)


def _inject_audio(context, fp: Dict) -> None:
    """Добавляет микро-шум к AudioContext для уникализации audio fingerprint."""
    noise = fp["audio_context_noise"]
    context.add_init_script(f"""
    (() => {{
        const _noise = {noise};
        if (typeof AudioBuffer !== 'undefined') {{
            const origGetChannelData = AudioBuffer.prototype.getChannelData;
            AudioBuffer.prototype.getChannelData = function(channel) {{
                const data = origGetChannelData.call(this, channel);
                if (this.__fp_patched) return data;
                this.__fp_patched = true;
                for (let i = 0; i < data.length; i += 100) {{
                    data[i] += _noise * 0.0001;
                }}
                return data;
            }};
        }}
    }})();
    """)


def _inject_fonts(context, fp: Dict) -> None:
    """Ограничивает список доступных шрифтов через document.fonts.check()."""
    fonts_json = json.dumps(fp["fonts"])
    context.add_init_script(f"""
    (() => {{
        const _allowed = new Set({fonts_json});
        if (document.fonts && document.fonts.check) {{
            const _origCheck = document.fonts.check.bind(document.fonts);
            document.fonts.check = function(font, text) {{
                const family = font.replace(/["\\']/g, '').split(',')[0].trim();
                if (!_allowed.has(family)) return false;
                try {{ return _origCheck(font, text); }} catch(_) {{ return false; }}
            }};
        }}
    }})();
    """)
