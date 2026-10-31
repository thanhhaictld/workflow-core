import type { NextConfig } from "next";

const upstream = (process.env.WORKFLOW_API_URL || "http://localhost:5000").replace(/\/$/, "");

const nextConfig: NextConfig = {
  async rewrites() {
    return [{ source: "/backend/:path*", destination: `${upstream}/api/:path*` }];
  }
};

export default nextConfig;
