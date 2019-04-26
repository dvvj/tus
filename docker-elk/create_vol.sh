docker volume create --driver local \
  --opt type=none \
  --opt device=$2 \
  --opt o=bind \
  $1
