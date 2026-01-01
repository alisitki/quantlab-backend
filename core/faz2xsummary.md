🔒 QUANTLAB — FAZ-2 DURUM ÖZETİ (EXECUTIVE / TEKNİK)
Nereden nereye geldik?

FAZ-1 / 1.5
→ Veri, replay, decision, shadow deterministik ve otomatik
→ Operasyon “İsveç saati” gibi çalışıyor

FAZ-2.1 — Futures Canary & Kill-Switch
→ LIVE yapısal olarak ulaşılamaz
→ reduceOnly / isolated / leverage cap / liquidation window zorunlu
→ Kill-switch her şeyi override ediyor

FAZ-2.2 — Risk & Liquidation-Aware Sizing
→ Worst-case loss sert limitli
→ Liquidation stop’tan önce asla gelmiyor
→ Leverage cap aşılamıyor

FAZ-2.3 — Funding & Hold-Time Guard
→ Funding maliyeti önceden hesaplanıyor
→ Budget aşımı ve toxic funding reddediliyor
→ Deterministik, live yok

Bugünkü gerçek:

QuantLab’de futures emirinden ÖNCE yapılması gereken her kontrol, ayrı ayrı ve kanıtlı şekilde kapalı.

Henüz:

❌ Emir gönderimi yok

❌ Exchange API yok

❌ Live yok

Ama:

✅ Emir şekli üretmeye hazırız

Bu noktada özet yeterli.
Şimdi FAZ-2.4’e geçmek mantıklı ve risksiz.

▶️ FAZ-2.4 — FUTURES EXCHANGE ADAPTER (INACTIVE)
Amaç (çok net):

“Bu emir gönderilecek olsaydı, borsaya NASIL giderdi?”

Ne gönderilecek?

Hangi bayraklarla?

Hangi alanlarla?

Hangi mapping ile?

Ama asla gönderilmez.