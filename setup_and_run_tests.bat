# Скопируй скачанные файлы в папку tests/
copy test_pipeline.py C:\Users\lemon\Documents\GitHub\ShortsProject\tests\
copy conftest.py C:\Users\lemon\Documents\GitHub\ShortsProject\tests\

# Коммить и пушь
cd C:\Users\lemon\Documents\GitHub\ShortsProject
git add tests\
git commit -m "test: добавлены реальные тесты (55 тест-кейсов, 9 модулей)"
git push origin main