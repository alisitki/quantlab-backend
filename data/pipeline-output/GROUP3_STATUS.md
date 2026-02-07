# GRUP 3 Status: BLOCKED - Bug Investigation Needed

## Problem
DiscoveryDataLoader tüm 234K event'i işliyor ama **tüm event'lerde features=null** döndürüyor.

## Debug Findings
- ✅ S3'ten parquet indirildi (5.3 MB, 234K events)
- ✅ ReplayEngine event'leri başarıyla okuyor
- ✅ Event format doğru: `{ symbol: 'ADAUSDT', bid_price, ask_price, bid_qty, ask_qty }`
- ✅ İzole test ÇALIŞIYOR (10K event → 10K feature)
- ❌ Pipeline'da TÜM event'ler warmup (features=null)

## Investigation Needed
Feature'lardan biri sürekli null döndürüyor ama hangisi belirsiz.
FeatureBuilder.onEvent() içinde hangi feature'ın null döndürdüğünü log'lamak gerekiyor.

## Next Steps
1. FeatureBuilder'a debug logging ekle
2. Hangi feature'ın null döndürdüğünü belirle
3. O feature'ın warmup logic'ini incele
4. Fix uygula ve pipeline'ı tekrar test et

## Workaround
Phase 9C geliştirmesi bu fix'i beklemeden devam edebilir (mock data ile test edilebilir).
