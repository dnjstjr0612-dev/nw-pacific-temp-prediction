# 1. local laptop
- code
- notebook
- sample test
- git push

# 2. server (Yonsei)
- raw data
- preprocess
- huge calculation
- processed/result

# 3. git 
### local 
git status git add . git commit -m "update preprocess" git push

### server
- cd ~/git
- cd ~/git/nw-pacific-temp-prediction

- find scripts -maxdepth 3 -type f : 서버에 어떤 스크립트 있는지 확인

# 4. get results to local
- only s size : scp leewonseok@acc.yonsei.ac.kr:/data3/leewonseok/processed/[file].nc

# 5. VENv
python3 -m venv ~/envs/geo
source ~/envs/geo/bin/activate
pip install --upgrade pip
pip install numpy xarray matplotlib netCDF4