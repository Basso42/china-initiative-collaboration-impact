sudo apt update
sudo apt install git-lfs

git lfs install
git clone https://huggingface.co/datasets/almanach/HALvest-Geometric

current_path=$(pwd)
mkdir data

cd HALvest-Geometric
unzip raw.zip -d "$current_path/data"

rm -rf HALvest-Geometric