# Corrección de Problema de Zona Horaria

## 🔴 Problema Identificado

Las fechas de eventos estaban guardándose con **6 horas de diferencia**:
- **SQL Server**: `2025-12-01 00:00:00.000` (UTC-6 / America/Mexico_City)
- **MySQL**: `2025-11-30 18:00:00` (UTC)

Esto afectaba a todos los campos de eventos de fechas.

## ✅ Solución Implementada

### 1. Script de Corrección (Una sola vez)

**Archivo:** `corregir_fechas_eventos.js`

Este script corrige las fechas **existentes** en la base de datos sumando 6 horas a todos los eventos.

**Ejecutar:**
```bash
npm run corregir-fechas
```

O directamente:
```bash
node corregir_fechas_eventos.js
```

**⚠️ IMPORTANTE:** Este script debe ejecutarse **SOLO UNA VEZ** para corregir los datos históricos.

**Campos que corrige:**
- APERTURA
- LLEGADA_MERCAN
- ENTREGA_CLASIFICA
- INICIO_CLASIFICA
- TERMINO_CLASIFICA
- INICIO_GLOSA
- TERMINO_GLOSA
- ENTREGA_GLOSA
- PAGO_PEDIMENTO
- DESPACHO_MERCAN
- ENTREGA_FAC
- FECHA_FAC
- ENTREGA_FAC_CLI
- ENTREGA_CAPTURA
- INICIO_CAPTURA
- TERMINO_CAPTURA
- PRIMER_RECONOCIMIENTO

### 2. Corrección en Scripts Principales (Permanente)

Se modificaron **todos los scripts** para usar la zona horaria correcta:

**Archivos modificados:**
- `index.js` (sincronización automática)
- `actualizar_campos.js`
- `actualizar_eventos.js`
- `actualizar_canceladas.js`

**Cambio realizado:**
```javascript
// ANTES:
await my.query("SET time_zone = '+00:00'"); // UTC

// AHORA:
await my.query("SET time_zone = '-06:00'"); // America/Mexico_City (UTC-6)
```

## 📋 Pasos para Aplicar la Corrección

### Paso 1: Corregir Datos Existentes
```bash
npm run corregir-fechas
```

Este proceso:
- Procesa todos los registros en lotes de 500
- Suma 6 horas a todas las fechas de eventos
- Muestra progreso en tiempo real
- Tarda aproximadamente 1-2 minutos por cada 10,000 registros

### Paso 2: Verificar la Corrección

Ejecuta esta consulta en MySQL para verificar:
```sql
SELECT 
  id_referencias,
  APERTURA,
  LLEGADA_MERCAN,
  PAGO_PEDIMENTO
FROM general
WHERE APERTURA IS NOT NULL
LIMIT 10;
```

Las fechas ahora deben coincidir con las de SQL Server.

### Paso 3: Reiniciar el Scheduler

Si el scheduler está corriendo, reinícialo para que use la nueva configuración:
```bash
# Detener el scheduler actual (Ctrl+C)
# Iniciar nuevamente:
npm start
```

## 🔄 Comportamiento Futuro

Desde ahora, **todas las sincronizaciones nuevas** guardarán las fechas correctamente en la zona horaria de México (UTC-6).

## ⚠️ Notas Importantes

1. **No ejecutar `corregir_fechas_eventos.js` más de una vez** - Sumaría 6 horas adicionales cada vez
2. Los scripts principales ya están corregidos para futuras sincronizaciones
3. El cambio es retrocompatible y no afecta la estructura de las tablas
4. Se recomienda hacer un respaldo de la base de datos antes de ejecutar la corrección

## 🧪 Prueba Rápida

Para verificar que todo funciona correctamente:

```bash
# 1. Ejecutar corrección de fechas
npm run corregir-fechas

# 2. Ejecutar sincronización manual
npm run run-once

# 3. Verificar que las nuevas fechas se guardan correctamente
```

## 📊 Monitoreo

El script de corrección muestra:
- Barra de progreso visual
- Tiempo transcurrido y estimado
- Número de registros actualizados
- Errores (si los hay)

Ejemplo de salida:
```
✅ Lote 10/50 completado
   [██████░░░░░░░░░░░░░░░░░░░░░░░░] 20.0%
   Tiempo transcurrido: 45.2 segundos
   Tiempo restante estimado: 3 min 1 seg
   Actualizados: 5000, Sin cambios: 0, Errores: 0
```
