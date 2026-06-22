"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

export default function Navigation() {
  const pathname = usePathname();

  return (
    <nav className="topbar">
      <div className="logo">
        <span className="logo-f1">F1</span>
        DASHBOARD
      </div>
      <div className="nav-tabs">
        <Link href="/" className={`nav-tab ${pathname === '/' ? 'active' : ''}`}>🏠 Overview</Link>
        <Link href="/drivers" className={`nav-tab ${pathname === '/drivers' ? 'active' : ''}`}>👨‍✈️ Drivers</Link>
        <Link href="/constructors" className={`nav-tab ${pathname === '/constructors' ? 'active' : ''}`}>🏎️ Constructors</Link>
        <Link href="/seasons" className={`nav-tab ${pathname === '/seasons' ? 'active' : ''}`}>📅 Seasons</Link>
        <Link href="/circuits" className={`nav-tab ${pathname === '/circuits' ? 'active' : ''}`}>🗺️ Circuits</Link>
        <Link href="/records" className={`nav-tab ${pathname === '/records' ? 'active' : ''}`}>🏆 Records</Link>
        <Link href="/compare" className={`nav-tab ${pathname === '/compare' ? 'active' : ''}`}>⚡ Compare</Link>
      </div>
      <div className="season-badge">1950 — 2024 · 75 SEASONS</div>
    </nav>
  );
}
