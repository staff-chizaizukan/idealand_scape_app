# sheets_writer.py
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
from googleapiclient.discovery import build
from gspread_formatting import (
    CellFormat,
    format_cell_range,
    TextFormat,
    Color,
)

import json
import re
import pandas as pd
import colorsys

def extract_spreadsheet_id(url) -> str:
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
    return m.group(1) if m else url


def write_sheet(spreadsheet_url, sheet_name, service_account_info, df_master, style_config):
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(service_account_info, scope)
        client = gspread.authorize(creds)

        # --- Open spreadsheet and worksheet ---
        spreadsheet_id = extract_spreadsheet_id(spreadsheet_url)
        spreadsheet = client.open_by_key(spreadsheet_id)
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=100, cols=26)

        # --- Clear and write DataFrame ---
        worksheet.clear()
        set_with_dataframe(worksheet, df_master, include_column_header=True, resize=True)
        reset_sheet(worksheet)
        base_sheet_design(worksheet, df_master)

        header_cfg = style_config.get("header", {})
        apply_header_style(
            worksheet,
            df_master,
            backgroundColor=header_cfg.get("backgroundColor", "#356854"),
            textColor=header_cfg.get("textColor", "#FFFFFF"),
            bold=header_cfg.get("bold", True),
            fontSize=header_cfg.get("fontSize", 10),
            header_height_px=header_cfg.get("header_height_px", 40),
        )
        apply_filter_to_header(worksheet, df_master)
        apply_wrap_text_to_header_row(worksheet, df_master)

        planet_cfg = style_config.get("planet", {})
        apply_planet_border(
            worksheet,
            df_master,
            has_planet=planet_cfg.get("has_planet", True),
            planet_color=planet_cfg.get("planet_color", "#356854"),
            start_row=planet_cfg.get("start_row", 1),
            start_col=planet_cfg.get("start_col", 1),
        )

        dropdowns(worksheet, df_master)

        column_cfg = style_config.get("columns", {})
        for col_key, params in column_cfg.items():
            style_column(worksheet, df_master, col_key, **params)

        print(f"✅ Successfully wrote data to '{sheet_name}' in spreadsheet {spreadsheet_id}")
        return worksheet.url, None

    except Exception as e:
        print(f"❌ Failed to write to sheet: {e}")
        return None, str(e)


def reset_sheet(worksheet):
    spreadsheet = worksheet.spreadsheet
    service = build("sheets", "v4", credentials=spreadsheet.client.auth)
    spreadsheet_id = spreadsheet.id
    sheet_id = worksheet.id

    # 現在の範囲サイズを取得
    data = worksheet.get_all_values()
    num_rows = max(1, len(data))
    num_cols = max(1, len(data[0]) if data else 1)

    # --- 1️⃣ データ検証削除 ---
    clear_data_validation = {"clearBasicFilter": {"sheetId": sheet_id}}

    # --- 2️⃣ 条件付き書式削除 ---
    try:
        rules = service.spreadsheets().get(
            spreadsheetId=spreadsheet_id, fields="sheets.conditionalFormats"
        ).execute()
        num_rules = 0
        for s in rules.get("sheets", []):
            if "conditionalFormats" in s:
                num_rules += len(s["conditionalFormats"])
    except Exception:
        num_rules = 0

    delete_rules = []
    for _ in range(num_rules):
        delete_rules.append({
            "deleteConditionalFormatRule": {"sheetId": sheet_id, "index": 0}
        })

    # --- 3️⃣ 全書式クリア + ベースフォント/カラー設定 ---
    base_text_color = {"red": 67/255, "green": 67/255, "blue": 67/255}
    base_text_format = {
        "fontFamily": "Roboto",
        "fontSize": 10,
        "foregroundColor": base_text_color,
        "bold": False,
        "italic": False,
    }

    clear_and_set_format = {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 0,
                "endRowIndex": num_rows,
                "startColumnIndex": 0,
                "endColumnIndex": num_cols,
            },
            "cell": {
                "userEnteredFormat": {
                    "textFormat": base_text_format,
                    "horizontalAlignment": "LEFT",
                    "verticalAlignment": "MIDDLE",
                    "backgroundColor": {"red": 1, "green": 1, "blue": 1},  # 白背景で統一
                    "wrapStrategy": "OVERFLOW_CELL"  # テキスト折返しをリセット
                }
            },
            "fields": "userEnteredFormat",
        }
    }

    # --- 4️⃣ 枠線リセット ---
    clear_borders = {
        "updateBorders": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 0,
                "endRowIndex": num_rows,
                "startColumnIndex": 0,
                "endColumnIndex": num_cols,
            },
            "top": {"style": "NONE"},
            "bottom": {"style": "NONE"},
            "left": {"style": "NONE"},
            "right": {"style": "NONE"},
            "innerHorizontal": {"style": "NONE"},
            "innerVertical": {"style": "NONE"},
        }
    }

    # 一括実行
    requests = [clear_data_validation, clear_and_set_format, clear_borders] + delete_rules

    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body={"requests": requests}
    ).execute()

    print("✅ Sheet formatting reset + base style applied (Roboto + #434343)")


def _hex_to_color(x: str):
    x = x.strip()
    if not x.startswith("#"):
        raise ValueError("Color must be hex like #RRGGBB")
    x = x[1:]
    if len(x) == 3:
        x = "".join([c*2 for c in x])
    r = int(x[0:2], 16) / 255.0
    g = int(x[2:4], 16) / 255.0
    b = int(x[4:6], 16) / 255.0
    return {"red": r, "green": g, "blue": b}

_COL_LET_RE = re.compile(r"^[A-Za-z]+$")

def _col_to_index(col_key, df):
    if isinstance(col_key, int):
        return col_key - 1
    if isinstance(col_key, str) and _COL_LET_RE.match(col_key):
        s = col_key.upper()
        idx = 0
        for c in s:
            idx = idx * 26 + (ord(c) - 64)
        return idx - 1
    if isinstance(col_key, str) and col_key in list(df.columns):
        return list(df.columns).index(col_key)
    raise ValueError(f"Unknown column spec: {col_key}")

def style_column(
    worksheet,
    df,
    col,
    *,
    fontFamily: str = "Roboto",
    fontSize: int = 10,
    bold: bool = False,
    italic: bool = False,
    foregroundColor: str = "#434343",
    # 背景は触らない（後方互換のため受け取るが無視）
    backgroundColor: str | None = None,
    wrap: bool | str = False,             # True/False or "WRAP"/"CLIP"/"OVERFLOW"
    horizontal: str = "LEFT",             # "LEFT"/"CENTER"/"RIGHT"
    vertical: str = "MIDDLE",             # "TOP"/"MIDDLE"/"BOTTOM"
    columnWidth: int | None = None,       # px
    exclude_header: bool = True,
    numberFormat: str | None = None       # "PERCENT" / "NUMBER" / "CURRENCY" など
):
    """
    指定列にスタイル + 列幅（任意）を適用。背景色は一切変更しない。
    1行目（ヘッダー）は exclude_header=True のとき除外。
    """
    if df is None or df.empty:
        return

    col_idx = _col_to_index(col, df)
    num_rows = len(df) + 1
    start_row = 1 if exclude_header else 0
    end_row = num_rows

    # wrap 正規化
    if isinstance(wrap, bool):
        wrap_mode = "WRAP" if wrap else "OVERFLOW_CELL"
    else:
        wm = wrap.upper()
        if wm == "OVERFLOW":
            wm = "OVERFLOW_CELL"
        if wm not in {"WRAP", "CLIP", "OVERFLOW_CELL"}:
            raise ValueError("wrap must be bool or 'WRAP'/'CLIP'/'OVERFLOW'")
        wrap_mode = wm

    # 文字色
    fg = _hex_to_color(foregroundColor) if isinstance(foregroundColor, str) else foregroundColor

    # ここで背景はセットしない（= 現状維持）
    fmt = {
        "textFormat": {
            "fontFamily": fontFamily,
            "fontSize": int(fontSize),
            "bold": bool(bold),
            "italic": bool(italic),
            "foregroundColor": fg,
        },
        "horizontalAlignment": horizontal.upper(),
        "verticalAlignment": vertical.upper(),
        "wrapStrategy": wrap_mode,
    }

    # 数値フォーマット（任意）
    fields = ["userEnteredFormat.textFormat",
              "userEnteredFormat.horizontalAlignment",
              "userEnteredFormat.verticalAlignment",
              "userEnteredFormat.wrapStrategy"]
    if numberFormat:
        fmt_type = numberFormat.upper()
        if fmt_type == "PERCENT":
            fmt["numberFormat"] = {"type": "PERCENT", "pattern": "0.00%"}
        elif fmt_type == "NUMBER":
            fmt["numberFormat"] = {"type": "NUMBER", "pattern": "0.00"}
        elif fmt_type == "CURRENCY":
            fmt["numberFormat"] = {"type": "CURRENCY", "pattern": "¥#,##0.00"}
        else:
            fmt["numberFormat"] = {"type": fmt_type}
        fields.append("userEnteredFormat.numberFormat")

    service = build("sheets", "v4", credentials=worksheet.spreadsheet.client.auth)

    requests = []
    # スタイル適用（背景を含まない fields だけ指定）
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": worksheet.id,
                "startRowIndex": start_row,
                "endRowIndex": end_row,
                "startColumnIndex": col_idx,
                "endColumnIndex": col_idx + 1,
            },
            "cell": {"userEnteredFormat": fmt},
            "fields": ",".join(fields),
        }
    })

    # 列幅（任意）
    if columnWidth is not None and int(columnWidth) > 0:
        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": worksheet.id,
                    "dimension": "COLUMNS",
                    "startIndex": col_idx,
                    "endIndex": col_idx + 1,
                },
                "properties": {"pixelSize": int(columnWidth)},
                "fields": "pixelSize",
            }
        })

    service.spreadsheets().batchUpdate(
        spreadsheetId=worksheet.spreadsheet.id,
        body={"requests": requests}
    ).execute()


def base_sheet_design(worksheet, df):
    """全体の背景・縦揃え・交互色設定"""
    if df.empty:
        return

    spreadsheet = worksheet.spreadsheet
    service = build("sheets", "v4", credentials=spreadsheet.client.auth)

    num_rows = len(df) + 1
    num_cols = len(df.columns)

    light_gray = {"red": 246/255, "green": 248/255, "blue": 249/255}

    requests = []

    # 縦中央揃え
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": worksheet.id,
                "startRowIndex": 1,
                "endRowIndex": num_rows,
                "startColumnIndex": 0,
                "endColumnIndex": num_cols,
            },
            "cell": {"userEnteredFormat": {"verticalAlignment": "MIDDLE"}},
            "fields": "userEnteredFormat.verticalAlignment",
        }
    })

    # 交互の背景色（2行目以降）
    for i in range(1, num_rows):
        if i % 2 == 0:
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": i,
                        "endRowIndex": i + 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": num_cols,
                    },
                    "cell": {"userEnteredFormat": {"backgroundColor": light_gray}},
                    "fields": "userEnteredFormat.backgroundColor",
                }
            })

    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet.id, body={"requests": requests}
    ).execute()


def dropdowns(worksheet, df):
    """
    C列: Smart Dropdown（淡い背景＋同系色文字）
    D列: 値が入っている行にだけ Smart Dropdown を付与（背景は触らない／文字は #666666）
         "nan"/"None" はシート上から消去（空文字に置換）
    """
    if df.empty:
        return

    spreadsheet = worksheet.spreadsheet
    service = build("sheets", "v4", credentials=spreadsheet.client.auth)
    num_rows = len(df) + 1  # ヘッダー含む

    # ---------------------------
    # C列：淡い背景にトーンダウン（lを上げる）
    # ---------------------------
    try:
        c_series = df.iloc[:, 2]
    except Exception:
        c_series = None

    if c_series is not None:
        categories_c = sorted(set([
            s for s in (str(v).strip() for v in c_series.dropna())
            if s not in ("", "None", "nan")
        ]))

        if categories_c:
            col_c = 2  # C
            # data validation
            reqs_c = [{
                "setDataValidation": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": 1,
                        "endRowIndex": num_rows,
                        "startColumnIndex": col_c,
                        "endColumnIndex": col_c + 1,
                    },
                    "rule": {
                        "condition": {
                            "type": "ONE_OF_LIST",
                            "values": [{"userEnteredValue": v} for v in categories_c],
                        },
                        "showCustomUi": True,
                        "strict": True,
                    },
                }
            }]

            # 色ユーティリティ
            def hsl_to_rgb(h, s, l):
                r, g, b = colorsys.hls_to_rgb(h, l, s)
                return {"red": r, "green": g, "blue": b}

            def text_color_from(h, s, l):
                # 背景をかなり淡く (l=0.94, s=0.38) にし、文字は同系色で濃く
                text_l = max(0, l - 0.65)
                text_s = min(1, s + 0.25)
                return hsl_to_rgb(h, text_s, text_l)

            n = max(1, len(categories_c))
            bg_palette  = [hsl_to_rgb(i / n, 0.38, 0.94) for i in range(n)]  # ←さらに淡く
            txt_palette = [text_color_from(i / n, 0.38, 0.94) for i in range(n)]

            for idx, cat in enumerate(categories_c):
                reqs_c.append({
                    "addConditionalFormatRule": {
                        "rule": {
                            "ranges": [{
                                "sheetId": worksheet.id,
                                "startRowIndex": 1,
                                "endRowIndex": num_rows,
                                "startColumnIndex": col_c,
                                "endColumnIndex": col_c + 1,
                            }],
                            "booleanRule": {
                                "condition": {
                                    "type": "TEXT_EQ",
                                    "values": [{"userEnteredValue": cat}],
                                },
                                "format": {
                                    "backgroundColor": bg_palette[idx],
                                    "textFormat": {"foregroundColor": txt_palette[idx], "bold": True},
                                },
                            },
                        },
                        "index": 0,
                    }
                })

            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet.id, body={"requests": reqs_c}
            ).execute()

    # ---------------------------
    # D列："nan"/"None" を空白化 → 非空行のみにプルダウン／#666666を適用
    # ---------------------------
    try:
        d_series = df.iloc[:, 3]
    except Exception:
        d_series = None

    if d_series is not None:
        # 1) まずシート上の "nan" / "None" を空文字に置換（全域）
        col_d = 3  # D
        d_range = {
            "sheetId": worksheet.id,
            "startRowIndex": 1,
            "endRowIndex": num_rows,
            "startColumnIndex": col_d,
            "endColumnIndex": col_d + 1,
        }
        cleanup_reqs = [
            {
                "findReplace": {
                    "range": d_range,
                    "find": "nan",
                    "replacement": "",
                    "matchCase": False,
                    "matchEntireCell": True,
                    "searchByRegex": False,
                }
            },
            {
                "findReplace": {
                    "range": d_range,
                    "find": "None",
                    "replacement": "",
                    "matchCase": False,
                    "matchEntireCell": True,
                    "searchByRegex": False,
                }
            },
        ]
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet.id, body={"requests": cleanup_reqs}
        ).execute()

        # 2) Python側の d_series から非空行を抽出（空白/None/nan 除外）
        non_empty_rows = [i for i, v in enumerate(d_series, start=2)  # シート行番号（ヘッダー1なので+1 → +1でもう一段）
                          if str(v).strip() not in ("", "None", "nan")]

        # カテゴリ候補（プルダウンのリスト）も None/空白を除外
        d_categories = sorted(set([
            s for s in (str(v).strip() for v in d_series.dropna())
            if s not in ("", "None", "nan")
        ]))

        if non_empty_rows and d_categories:
            # 連続ブロックに圧縮してリクエスト数を抑制
            blocks = []
            start = prev = None
            for r in non_empty_rows:
                if start is None:
                    start = prev = r
                elif r == prev + 1:
                    prev = r
                else:
                    blocks.append((start, prev))
                    start = prev = r
            if start is not None:
                blocks.append((start, prev))

            # 3) 各ブロックにだけ DataValidation と テキスト色(#666666) を適用
            gray_text = {"red": 100/255, "green": 100/255, "blue": 100/255}
            reqs_d = []
            for (r1, r2) in blocks:
                reqs_d.append({
                    "setDataValidation": {
                        "range": {
                            "sheetId": worksheet.id,
                            "startRowIndex": r1 - 1,
                            "endRowIndex": r2,          # endは非包含なのでそのまま
                            "startColumnIndex": col_d,
                            "endColumnIndex": col_d + 1,
                        },
                        "rule": {
                            "condition": {
                                "type": "ONE_OF_LIST",
                                "values": [{"userEnteredValue": v} for v in d_categories],
                            },
                            "showCustomUi": True,
                            "strict": True,
                        },
                    }
                })
                reqs_d.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": worksheet.id,
                            "startRowIndex": r1 - 1,
                            "endRowIndex": r2,
                            "startColumnIndex": col_d,
                            "endColumnIndex": col_d + 1,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "textFormat": 
                                {
                                    "foregroundColor": gray_text,
                                               "bold": True  
                                }
                            }
                        },
                        "fields": "userEnteredFormat.textFormat",
                    }
                })

            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet.id, body={"requests": reqs_d}
            ).execute()
        # 非空行が無い場合はスルー（プルダウンも付けない）

def _hex_to_rgb_color(hex_color: str):
    """#RRGGBB → Google Sheets Color dict"""
    hex_color = hex_color.strip()
    if not hex_color.startswith("#"):
        raise ValueError("Color must start with '#' like #RRGGBB")
    hex_color = hex_color[1:]
    if len(hex_color) == 3:
        hex_color = "".join([c * 2 for c in hex_color])
    r = int(hex_color[0:2], 16) / 255.0
    g = int(hex_color[2:4], 16) / 255.0
    b = int(hex_color[4:6], 16) / 255.0
    return {"red": r, "green": g, "blue": b}

def apply_planet_border(
    worksheet,
    df,
    *,
    has_planet: bool = True,               # 惑星（外枠）を描くかどうか
    planet_color: str = "#356854",         # 惑星（外枠）の色（デフォルト:緑）
    start_row: int = 1,
    start_col: int = 1,
):
    """
    外枠・グループ線を惑星のように描画する。
    惑星の色と「そもそも惑星を作るかどうか」を制御可能。

    Args:
        worksheet: gspread Worksheet
        df: pandas DataFrame
        has_planet: True なら外枠を描画、False なら全て削除
        planet_color: 惑星カラー (#RRGGBB)
        start_row, start_col: 表の開始位置（1始まり）
    """
    if df.empty:
        return

    spreadsheet = worksheet.spreadsheet
    service = build("sheets", "v4", credentials=spreadsheet.client.auth)
    num_rows = len(df)
    num_cols = len(df.columns)

    color = _hex_to_rgb_color(planet_color)

    # --- まず全体の内側線を削除 ---
    clear_inner_lines = {
        "updateBorders": {
            "range": {
                "sheetId": worksheet.id,
                "startRowIndex": 0,
                "endRowIndex": num_rows + 1,
                "startColumnIndex": 0,
                "endColumnIndex": num_cols,
            },
            "innerHorizontal": {"style": "NONE"},
            "innerVertical": {"style": "NONE"},
        }
    }

    # 枠線を描かない場合（惑星を消す）
    if not has_planet:
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet.id, body={"requests": [clear_inner_lines]}
        ).execute()
        print("🪐 Planet border removed.")
        return

    # --- 外枠を描く ---
    draw_outer_borders = {
        "updateBorders": {
            "range": {
                "sheetId": worksheet.id,
                "startRowIndex": start_row - 1,
                "endRowIndex": start_row - 1 + num_rows + 1,
                "startColumnIndex": start_col - 1,
                "endColumnIndex": start_col - 1 + num_cols,
            },
            "top": {"style": "SOLID", "width": 2, "color": color},
            "bottom": {"style": "SOLID", "width": 2, "color": color},
            "left": {"style": "SOLID", "width": 2, "color": color},
            "right": {"style": "SOLID", "width": 2, "color": color},
        }
    }

    # --- グループ境界線を追加 ---
    group_right_edges = [5, 10, 12, 15, 18, 21]
    group_lines = []
    for edge_index in group_right_edges:
        group_lines.append({
            "updateBorders": {
                "range": {
                    "sheetId": worksheet.id,
                    "startRowIndex": 0,
                    "endRowIndex": num_rows + 1,
                    "startColumnIndex": edge_index,
                    "endColumnIndex": edge_index + 1,
                },
                "left": {"style": "SOLID", "width": 2, "color": color},
            }
        })

    # --- リクエスト順（内側削除 → 外枠 → グループ線） ---
    requests = [clear_inner_lines, draw_outer_borders] + group_lines

    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet.id, body={"requests": requests}
    ).execute()

    print(f"🪐 Planet border applied in color {planet_color}")

# ===============================
# 🟩 1行目ヘッダーを緑背景＋白文字＋太字にする
# ===============================
def apply_header_style(
    worksheet,
    df,
    *,
    backgroundColor: str = "#356854",     # デフォルト緑
    textColor: str = "#FFFFFF",           # デフォルト白
    bold: bool = True,                    # デフォルト太字ON
    fontSize: int = 10,                   # 文字サイズ
    header_height_px: int = 40            # 行の高さ
):
    """
    1行目（ヘッダー）にスタイルを適用：
      - 背景色、文字色、太字、文字サイズ、行高さを指定可能
      - 1行目を固定

    例:
      apply_header_style_green(ws, df, backgroundColor="#004D40", textColor="#FFE082", bold=False, fontSize=12, header_height_px=50)
    """
    if df.empty:
        return

    spreadsheet = worksheet.spreadsheet
    service = build("sheets", "v4", credentials=spreadsheet.client.auth)

    num_cols = len(df.columns)
    # 最終列を "A1:Z1" のような文字列に変換
    if num_cols <= 26:
        last_col_letter = chr(64 + num_cols)
    else:
        last_col_letter = ""
        n = num_cols
        while n > 0:
            n, remainder = divmod(n - 1, 26)
            last_col_letter = chr(65 + remainder) + last_col_letter

    header_range = f"A1:{last_col_letter}1"

    # --- スタイル設定 ---
    bg_color = _hex_to_color(backgroundColor)
    fg_color = _hex_to_color(textColor)

    header_format = CellFormat(
        backgroundColor=bg_color,
        textFormat=TextFormat(
            bold=bold,
            foregroundColor=fg_color,
            fontSize=fontSize
        ),
        horizontalAlignment="CENTER",
        verticalAlignment="MIDDLE",
    )

    # --- フォーマット適用 ---
    format_cell_range(worksheet, header_range, header_format)

    # --- 固定 & 高さ変更 ---
    requests = [
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": worksheet.id,
                    "gridProperties": {"frozenRowCount": 1},
                },
                "fields": "gridProperties.frozenRowCount",
            }
        },
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": worksheet.id,
                    "dimension": "ROWS",
                    "startIndex": 0,  # 1行目はインデックス0
                    "endIndex": 1,
                },
                "properties": {"pixelSize": int(header_height_px)},
                "fields": "pixelSize",
            }
        },
    ]

    # --- 一括リクエスト実行 ---
    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet.id, body={"requests": requests}
    ).execute()

    print(
        f"✅ Header style applied (bg={backgroundColor}, text={textColor}, bold={bold}, size={fontSize}, height={header_height_px}px)"
    )

# ===============================
# 🔍 フィルターを1行目に適用
# ===============================
def apply_filter_to_header(worksheet, df):
    """シートの1行目にフィルターを設定"""
    if df.empty:
        return

    spreadsheet = worksheet.spreadsheet
    service = build("sheets", "v4", credentials=spreadsheet.client.auth)

    num_cols = len(df.columns)
    request_body = {
        "requests": [
            {
                "setBasicFilter": {
                    "filter": {
                        "range": {
                            "sheetId": worksheet.id,
                            "startRowIndex": 0,
                            "endRowIndex": len(df) + 1,
                            "startColumnIndex": 0,
                            "endColumnIndex": num_cols,
                        }
                    }
                }
            }
        ]
    }
    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet.id, body=request_body
    ).execute()

# ===============================
# 🔤 1行目すべてのセルを折り返し表示
# ===============================
def apply_wrap_text_to_header_row(worksheet, df):
    """1行目（ヘッダー行）の全列に折り返し設定を適用"""
    if df.empty:
        return

    num_cols = len(df.columns)
    spreadsheet = worksheet.spreadsheet
    service = build("sheets", "v4", credentials=spreadsheet.client.auth)

    request_body = {
        "requests": [
            {
                "repeatCell": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": num_cols,
                    },
                    "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP"}},
                    "fields": "userEnteredFormat.wrapStrategy",
                }
            }
        ]
    }

    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet.id, body=request_body
    ).execute()

