export function getFlagEmoji(nationality: string | null | undefined): string {
  if (!nationality) return "🏁";
  
  const map: Record<string, string> = {
    'British': '🇬🇧',
    'German': '🇩🇪',
    'Finnish': '🇫🇮',
    'Brazilian': '🇧🇷',
    'Spanish': '🇪🇸',
    'Dutch': '🇳🇱',
    'French': '🇫🇷',
    'Italian': '🇮🇹',
    'Australian': '🇦🇺',
    'Austrian': '🇦🇹',
    'Monegasque': '🇲🇨',
    'Mexican': '🇲🇽',
    'Canadian': '🇨🇦',
    'Argentine': '🇦🇷',
    'American': '🇺🇸',
    'Japanese': '🇯🇵',
    'Swiss': '🇨🇭',
    'Belgian': '🇧🇪',
    'Swedish': '🇸🇪',
    'Danish': '🇩🇰',
    'Russian': '🇷🇺',
    'New Zealander': '🇳🇿',
    'Colombian': '🇨🇴',
    'Venezuelan': '🇻🇪',
    'South African': '🇿🇦',
    'Polish': '🇵🇱',
    'Thai': '🇹🇭',
    'Chinese': '🇨🇳'
  };

  return map[nationality] || "🏁";
}
