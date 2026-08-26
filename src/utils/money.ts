function safeMoneyValue(value: unknown, fieldName = 'desconocido'): number | null {
  try {
    if (value === null || value === undefined) return null;

    if (typeof value === 'object') {
      const obj = value as Record<string, unknown>;
      if (obj['value'] !== undefined) {
        value = obj['value'];
      } else {
        return null;
      }
    }

    let numValue: number;
    if (typeof value === 'string') {
      numValue = parseFloat(value.replace(/[^\d.-]/g, ''));
    } else {
      numValue = Number(value);
    }

    if (isNaN(numValue)) return null;
    return numValue;
  } catch (err) {
    console.error(`Error procesando valor monetario para ${fieldName}:`, err);
    return null;
  }
}

export { safeMoneyValue };
