# Reset local lab

Dừng và giữ data:

```powershell
docker compose down
```

Dừng và xóa toàn bộ data:

```powershell
docker compose down -v
```

Build lại từ đầu:

```powershell
docker compose build --no-cache
docker compose up -d
```

Xem log:

```powershell
docker compose logs -f producer processor dashboard
```
