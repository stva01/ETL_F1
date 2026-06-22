import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  serverExternalPackages: ["duckdb"],
  allowedDevOrigins: [
    "localhost", 
    "127.0.0.1", 
    "5fb6-2405-201-3f-6802-19ab-a8b1-b37b-abb6.ngrok-free.app"
  ],
};

export default nextConfig;
