#include <metal_stdlib>
using namespace metal;

struct VertexOut {
    float4 position [[position]];
    float2 uv;
};

struct Uniforms {
    float2 resolution;
    float time;
    float destination;
    float provider;
    float sourceKind;
    float transcriptDensity;
    float sourceDensity;
    float readiness;
    float commandState;
    float isRunning;
    uint barCount;
    float commandDuration;
    float commandOutputScale;
};

vertex VertexOut workspaceVertex(uint vertexID [[vertex_id]]) {
    float2 positions[3] = {
        float2(-1.0, -1.0),
        float2(3.0, -1.0),
        float2(-1.0, 3.0)
    };

    VertexOut out;
    out.position = float4(positions[vertexID], 0.0, 1.0);
    out.uv = 0.5 * (positions[vertexID] + 1.0);
    return out;
}

float3 providerColor(float provider) {
    if (provider < 0.5) {
        return float3(0.95, 0.58, 0.31);
    }
    if (provider < 1.5) {
        return float3(0.29, 0.78, 0.99);
    }
    return float3(0.46, 0.89, 0.52);
}

float3 destinationBase(float destination) {
    if (destination < 0.5) {
        return float3(0.05, 0.08, 0.12);
    }
    if (destination < 1.5) {
        return float3(0.09, 0.07, 0.13);
    }
    if (destination < 2.5) {
        return float3(0.08, 0.10, 0.09);
    }
    return float3(0.07, 0.09, 0.11);
}

float3 commandTint(float commandState) {
    if (commandState < 0.5) {
        return float3(0.0);
    }
    if (commandState < 1.5) {
        return float3(0.28, 0.93, 0.56);
    }
    return float3(0.96, 0.34, 0.34);
}

fragment float4 workspaceFragment(
    VertexOut in [[stage_in]],
    constant Uniforms& uniforms [[buffer(0)]],
    constant float* bars [[buffer(1)]]
) {
    float2 uv = clamp(in.uv, 0.0, 1.0);

    float density = mix(6.0, 24.0, uniforms.transcriptDensity * 0.6 + uniforms.sourceDensity * 0.4);
    float2 grid = abs(fract(uv * density) - 0.5);
    float line = 1.0 - smoothstep(0.46, 0.50, min(grid.x, grid.y));

    float motion = uniforms.isRunning > 0.5
        ? 0.5 + 0.5 * sin((uv.x * 25.0) + (uv.y * 14.0) + uniforms.time * 2.3 + uniforms.destination)
        : 0.0;

    float dataField = 0.5 + 0.5 * sin((uv.x + uniforms.provider * 0.13) * 17.0)
        * cos((uv.y + uniforms.destination * 0.08) * 23.0 + uniforms.time * 0.35);
    dataField = smoothstep(0.72, 0.98, dataField);

    float sourcePulse = uniforms.sourceKind < -0.5
        ? 0.0
        : smoothstep(0.10, 0.95, 1.0 - distance(uv, float2(0.78, 0.24)) * 1.55);

    float readinessGlow = clamp(uniforms.readiness, 0.0, 1.0);
    float horizon = smoothstep(0.0, 1.0, uv.x) * smoothstep(1.0, 0.2, uv.y);

    float barField = 0.0;
    if (uniforms.barCount > 0) {
        float barPosition = clamp(uv.x * float(uniforms.barCount), 0.0, float(uniforms.barCount - 1));
        uint barIndex = min(uint(barPosition), uniforms.barCount - 1);
        float barHeight = bars[barIndex];
        float barThreshold = 0.92 - barHeight * 0.28;
        float scanline = 1.0 - smoothstep(barThreshold, barThreshold + 0.03, uv.y);
        barField = scanline * smoothstep(0.12, 0.95, barHeight);
    }

    float durationGlow = smoothstep(0.0, 1.0, min(uniforms.commandDuration / 1800.0, 1.0));
    float outputGlow = smoothstep(0.0, 1.0, uniforms.commandOutputScale);

    float3 accent = providerColor(uniforms.provider);
    float3 base = destinationBase(uniforms.destination);
    float3 status = commandTint(uniforms.commandState);

    float3 color = base;
    color += accent * (0.16 + sourcePulse * 0.16);
    color += accent * line * (0.05 + readinessGlow * 0.12);
    color += accent * dataField * (0.08 + readinessGlow * 0.10);
    color += accent * barField * (0.16 + uniforms.sourceDensity * 0.08 + uniforms.transcriptDensity * 0.06);
    color += status * horizon * (0.12 + durationGlow * 0.10 + outputGlow * 0.08);
    color += accent * motion * 0.10;

    float vignette = smoothstep(0.95, 0.18, distance(uv, float2(0.5, 0.45)));
    color *= 0.88 + vignette * 0.24;

    return float4(color, 1.0);
}
