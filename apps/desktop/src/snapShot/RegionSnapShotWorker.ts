// @effect-diagnostics nodeBuiltinImport:off -- The capture worker encodes PNGs with Node's zlib.
// Isolate native screenshot crashes and stalls in a Node-mode child.
import * as NodeZlib from "node:zlib";

import type * as Electron from "electron";

import type { RegionSnapShotRequest, RegionSnapShotResult } from "./RegionSnapShot.ts";

const DPI_AWARENESS_CONTEXT_PER_MONITOR_AWARE_V2 = -4n;
const SRCCOPY = 0x00cc0020;
const BITMAPINFOHEADER_SIZE = 40;

process.once("disconnect", () => process.exit(0));

function pngChunk(type: string, data: Buffer): Buffer {
  const chunk = Buffer.alloc(12 + data.length);
  chunk.writeUInt32BE(data.length, 0);
  chunk.write(type, 4, "latin1");
  data.copy(chunk, 8);
  chunk.writeUInt32BE(NodeZlib.crc32(chunk.subarray(4, 8 + data.length)), 8 + data.length);
  return chunk;
}

function encodeBgraPng(bgra: Buffer, width: number, height: number): Buffer {
  const stride = width * 3 + 1;
  const rows = Buffer.alloc(stride * height);
  for (let y = 0; y < height; y++) {
    let source = y * width * 4;
    let target = y * stride + 1;
    for (let x = 0; x < width; x++, source += 4, target += 3) {
      rows[target] = bgra[source + 2]!;
      rows[target + 1] = bgra[source + 1]!;
      rows[target + 2] = bgra[source]!;
    }
  }
  const header = Buffer.alloc(13);
  header.writeUInt32BE(width, 0);
  header.writeUInt32BE(height, 4);
  header[8] = 8;
  header[9] = 2;
  return Buffer.concat([
    Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
    pngChunk("IHDR", header),
    pngChunk("IDAT", NodeZlib.deflateSync(rows, { level: 3 })),
    pngChunk("IEND", Buffer.alloc(0)),
  ]);
}

// Copies physical desktop pixels, the space GetWindowRect reports. xa11y 0.13 scales a
// region by the monitor under its origin, which crops or shifts it on mixed-DPI desktops.
async function loadCapture() {
  const { DataType, load, open } = await import("ffi-rs");
  open({ library: "t3-capture-user32", path: "user32.dll" });
  open({ library: "t3-capture-gdi32", path: "gdi32.dll" });
  const call = (library: string, funcName: string, params: ReadonlyArray<bigint>) =>
    load({
      library: `t3-capture-${library}`,
      funcName,
      retType: DataType.BigInt,
      paramsType: params.map(() => DataType.BigInt),
      paramsValue: [...params],
    }) as bigint;

  return (region: Electron.Rectangle) => {
    const { x, y, width, height } = region;
    if (width <= 0 || height <= 0) throw new Error("Windows window capture region is empty.");
    call("user32", "SetThreadDpiAwarenessContext", [DPI_AWARENESS_CONTEXT_PER_MONITOR_AWARE_V2]);
    const screen = call("user32", "GetDC", [0n]);
    if (screen === 0n) throw new Error("Windows window capture could not open the desktop.");
    const memory = call("gdi32", "CreateCompatibleDC", [screen]);
    const bitmap = load({
      library: "t3-capture-gdi32",
      funcName: "CreateCompatibleBitmap",
      retType: DataType.BigInt,
      paramsType: [DataType.BigInt, DataType.I32, DataType.I32],
      paramsValue: [screen, width, height],
    }) as bigint;
    try {
      if (memory === 0n || bitmap === 0n) {
        throw new Error("Windows window capture could not allocate a bitmap.");
      }
      const previous = call("gdi32", "SelectObject", [memory, bitmap]);
      const copied = load({
        library: "t3-capture-gdi32",
        funcName: "BitBlt",
        retType: DataType.Boolean,
        paramsType: [
          DataType.BigInt,
          DataType.I32,
          DataType.I32,
          DataType.I32,
          DataType.I32,
          DataType.BigInt,
          DataType.I32,
          DataType.I32,
          DataType.U32,
        ],
        paramsValue: [memory, 0, 0, width, height, screen, x, y, SRCCOPY],
      });
      // GetDIBits requires the bitmap to be deselected first.
      call("gdi32", "SelectObject", [memory, previous]);
      if (!copied) throw new Error("Windows window capture failed to copy the screen.");

      const info = Buffer.alloc(BITMAPINFOHEADER_SIZE + 4);
      info.writeUInt32LE(BITMAPINFOHEADER_SIZE, 0);
      info.writeInt32LE(width, 4);
      // A negative height requests top-down rows.
      info.writeInt32LE(-height, 8);
      info.writeUInt16LE(1, 12);
      info.writeUInt16LE(32, 14);
      const pixels = Buffer.alloc(width * height * 4);
      const rows = load({
        library: "t3-capture-gdi32",
        funcName: "GetDIBits",
        retType: DataType.I32,
        paramsType: [
          DataType.BigInt,
          DataType.BigInt,
          DataType.U32,
          DataType.U32,
          DataType.U8Array,
          DataType.U8Array,
          DataType.U32,
        ],
        paramsValue: [memory, bitmap, 0, height, pixels, info, 0],
      });
      if (rows !== height) throw new Error("Windows window capture failed to read pixels.");
      return { width, height, png: encodeBgraPng(pixels, width, height) };
    } finally {
      if (bitmap !== 0n) call("gdi32", "DeleteObject", [bitmap]);
      if (memory !== 0n) call("gdi32", "DeleteDC", [memory]);
      call("user32", "ReleaseDC", [0n, screen]);
    }
  };
}

async function capture() {
  const captureRegion = await loadCapture();
  process.send?.("ready");
  const request = await new Promise<RegionSnapShotRequest>((resolve) => {
    process.once("message", resolve);
  });
  const shot = captureRegion(request.region);
  const result: RegionSnapShotResult = {
    type: "result",
    width: shot.width,
    height: shot.height,
    png: shot.png.toString("base64"),
  };
  process.send?.(result);
}

void capture().catch((error: unknown) =>
  process.send?.({
    type: "error",
    message: error instanceof Error ? error.message : "Windows window capture failed.",
  } satisfies RegionSnapShotResult),
);
