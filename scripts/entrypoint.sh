#!/bin/bash

# 1. Khởi động dịch vụ SSH server ngầm để các node giao tiếp được với nhau
sudo service ssh start

echo "=========================================================="
echo " Node [$(hostname)] role: ${NODE_ROLE:-worker} is ready! "
echo "=========================================================="

# 2. LỆNH SỐNG CÒN CỦA DOCKER: Giữ tiến trình foreground để container luôn chạy
tail -f /dev/null
