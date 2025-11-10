import requests
from bs4 import BeautifulSoup
import logging
from typing import Tuple, Optional
import os
import time

logger = logging.getLogger(__name__)

class HoroscopeParser:
    def __init__(self):
        self.zodiac_signs = {
            "овен": ("aries", "♈️"),
            "телец": ("taurus", "♉️"),
            "близнецы": ("gemini", "♊️"),
            "рак": ("cancer", "♋️"),
            "лев": ("leo", "♌️"),
            "дева": ("virgo", "♍️"),
            "весы": ("libra", "♎️"),
            "скорпион": ("scorpio", "♏️"),
            "стрелец": ("sagittarius", "♐️"),
            "козерог": ("capricorn", "♑️"),
            "водолей": ("aquarius", "♒️"),
            "рыбы": ("pisces", "♓️")
        }
        self.base_url = "https://horoscopes.rambler.ru/{}/today/"
        self.tarot_url = "https://horoscopes.rambler.ru/taro/"
        self.tarot_images_path = "/root/TAROBOT/tarot_images"
        self.test_horoscopes()

    def get_horoscope(self, sign: str) -> str:
        """
        Получает гороскоп для знака зодиака.
        Args:
            sign: название знака зодиака
        Returns:
            отформатированный текст гороскопа
        """
        try:
            sign = sign.lower()
            if sign not in self.zodiac_signs:
                logger.error(f"Invalid zodiac sign: {sign}")
                return "Неверный знак зодиака"

            sign_en, sign_symbol = self.zodiac_signs[sign]
            url = self.base_url.format(sign_en)
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
            logger.info(f"Fetching horoscope from URL: {url}")

            for attempt in range(3):
                try:
                    response = requests.get(url, headers=headers, timeout=10)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.text, 'html.parser')
                    horoscope_p = None
                    possible_selectors = [
                        'p[class*="_5yHoW"]',
                        'p[class*="AjIPq"]',
                        'p[class*="horoscope-text"]',
                        'p[class*="article-text"]',
                        'div[class*="content"] p',
                        'article p',
                        'div[class*="text-block"] p',
                        'p'
                    ]

                    for selector in possible_selectors:
                        horoscope_p = soup.select_one(selector)
                        if horoscope_p and horoscope_p.text.strip():
                            break

                    if horoscope_p and horoscope_p.text.strip() and len(horoscope_p.text.strip()) > 50:
                        horoscope_text = horoscope_p.text.strip()
                        logger.info(f"Successfully found horoscope text for {sign}")
                        return horoscope_text
                    break
                except requests.RequestException as e:
                    if attempt < 2:
                        logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in 5 seconds...")
                        time.sleep(5)
                    else:
                        logger.error(f"Network error after 3 attempts for {sign}: {e}")
                        return "Извините, не удалось подключиться к сайту. Проверьте DNS или доступность сайта."

            logger.error(f"No horoscope text found for {sign}")
            return "Извините, не удалось получить гороскоп. Попробуйте позже."

        except Exception as e:
            logger.error(f"Unexpected error while fetching horoscope for {sign}: {e}")
            return "Извините, произошла непредвиденная ошибка. Попробуйте позже."

    def test_horoscopes(self):
        """Тестирует получение гороскопов для всех знаков зодиака"""
        logger.info("=== Начинаем тестирование гороскопов для всех знаков ===")
        results = {"success": [], "failed": []}

        for sign in self.zodiac_signs.keys():
            logger.info(f"\n=== Тестируем знак: {sign.upper()} ===")
            horoscope = self.get_horoscope(sign)
            if "Извините" not in horoscope and len(horoscope) > 100:
                results["success"].append(sign)
                logger.info(f"✅ Успешно получен гороскоп для {sign}")
                logger.info(f"Текст гороскопа: {horoscope[:100]}...")
            else:
                results["failed"].append(sign)
                logger.error(f"❌ Не удалось получить гороскоп для {sign}")
                logger.error(f"Полученный текст: {horoscope}")

        logger.info("\n=== Результаты тестирования ===")
        logger.info(f"✅ Успешно: {len(results['success'])} знаков")
        if results['success']:
            logger.info(f"Работает для знаков: {', '.join(results['success'])}")
        logger.info(f"❌ Неудачно: {len(results['failed'])} знаков")
        if results['failed']:
            logger.info(f"Не работает для знаков: {', '.join(results['failed'])}")
        logger.info("=== Тестирование завершено ===")
        self.test_results = results

    def get_tarot(self) -> Tuple[str, str, Optional[str]]:
        """
        Получает карту Таро дня
        Returns: (название карты, отформатированный текст с описанием, путь к локальному файлу изображения или None)
        """
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
            for attempt in range(3):
                try:
                    response = requests.get(self.tarot_url, headers=headers, timeout=10)
                    response.raise_for_status()
                    break
                except requests.RequestException as e:
                    if attempt < 2:
                        logger.warning(f"Attempt {attempt + 1} failed for tarot: {e}. Retrying in 5 seconds...")
                        time.sleep(5)
                    else:
                        logger.error(f"Network error after 3 attempts for tarot: {e}")
                        return "Карта дня", "✨Карта дня✨\n📬 Не удалось подключиться к сайту для Таро.", None

            soup = BeautifulSoup(response.text, 'html.parser')

            # Поиск заголовка карты по указанному классу
            title_tag = soup.select_one('h2.h7yAL.xAIf2')
            card_title = title_tag.text.strip() if title_tag else "Карта дня"
            logger.debug(f"Extracted card_title: {card_title}")

            # Извлечение чистого имени карты
            card_name = card_title
            if ":" in card_name:
                card_name = card_name.split(":")[1].strip()
            elif "Карта Таро сегодня" in card_name:
                card_name = card_name.replace("Карта Таро сегодня", "").strip()
            card_name = card_name.replace("ё", "е").strip()
            logger.debug(f"Extracted card_name: {card_name}")

            # Поиск описания по указанному классу
            description_tag = soup.select_one('div.oZxor.vAzqt[itemprop="articleBody"]')
            card_description = "\n\n".join(p.text.strip() for p in description_tag.find_all('p') if p.text.strip()) if description_tag else "Описание недоступно"

            # Обработка имени файла изображения (без вывода)
            file_name_base = card_name.lower().replace(" ", "_")
            file_name_corrections = {
                "туз кубков": "туз_кубков.png",
                "двойка кубков": "двойка_кубков.png",
                "тройка кубков": "тройка_кубков.png",
                "четверка кубков": "четверка_кубков.png",
                "пятерка кубков": "пятерка_кубков.png",
                "шестерка кубков": "шестерка_кубков.png",
                "семерка кубков": "семерка_кубков.png",
                "восьмерка кубков": "восьмерка_кубков.png",
                "девятка кубков": "девятка_кубков.png",
                "десятка кубков": "десятка_кубков.png",
                "паж кубков": "паж_кубков.png",
                "рыцарь кубков": "рыцарь_кубков.png",
                "королева кубков": "королева_кубков.png",
                "король кубков": "король_кубков.png",
                "туз мечей": "туз_мечей.png",
                "двойка мечей": "двойка_мечей.png",
                "тройка мечей": "тройка_мечей.png",
                "четверка мечей": "четверка_мечей.png",
                "пятерка мечей": "пятерка_мечей.png",
                "шестерка мечей": "шестерка_мечей.png",
                "семерка мечей": "семерка_мечей.png",
                "восьмерка мечей": "восьмерка_мечей.png",
                "девятка мечей": "девятка_мечей.png",
                "десятка мечей": "десятка_мечей.png",
                "паж мечей": "паж_мечей.png",
                "рыцарь мечей": "рыцарь_мечей.png",
                "королева мечей": "королева_мечей.png",
                "король мечей": "король_мечей.png",
                "туз посохов": "туз_посохов.png",
                "двойка посохов": "двойка_посохов.png",
                "тройка посохов": "тройка_посохов.png",
                "четверка посохов": "четверка_посохов.png",
                "пятерка посохов": "пятерка_посохов.png",
                "шестерка посохов": "шестерка_посохов.png",
                "семерка посохов": "семерка_посохов.png",
                "восьмерка посохов": "восьмерка_посохов.png",
                "девятка посохов": "девятка_посохов.png",
                "десятка посохов": "десятка_посохов.png",
                "паж посохов": "паж_посохов.png",
                "рыцарь посохов": "рыцарь_посохов.png",
                "королева посохов": "королева_посохов.png",
                "король посохов": "король_посохов.png",
                "туз пентаклей": "туз_пентаклей.png",
                "двойка пентаклей": "двойка_пентаклей.png",
                "тройка пентаклей": "тройка_пентаклей.png",
                "четверка пентаклей": "четверка_пентаклей.png",
                "пятерка пентаклей": "пятерка_пентаклей.png",
                "шестерка пентаклей": "шестерка_пентаклей.png",
                "семерка пентаклей": "семерка_пентаклей.png",
                "восьмерка пентаклей": "восьмерка_пентаклей.png",
                "девятка пентаклей": "девятка_пентаклей.png",
                "десятка пентаклей": "десятка_пентаклей.png",
                "паж пентаклей": "паж_пентаклей.png",
                "рыцарь пентаклей": "рыцарь_пентаклей.png",
                "королева пентаклей": "королева_пентаклей.png",
                "король пентаклей": "король_пентаклей.png",
                "шут": "шут.png",
                "маг": "маг.png",
                "жрица": "жрица.png",
                "императрица": "императрица.png",
                "император": "император.png",
                "иерофант": "иерофант.png",
                "влюбленные": "влюбленные.png",
                "колесница": "колесница.png",
                "сила": "сила.png",
                "отшельник": "отшельник.png",
                "колесо фортуны": "колесо_фортуны.png",
                "справедливость": "справедливость.png",
                "повешенный": "повешенный.png",
                "смерть": "смерть.png",
                "умеренность": "умеренность.png",
                "дьявол": "дьявол.png",
                "башня": "башня.png",
                "звезда": "звезда.png",
                "луна": "луна.png",
                "солнце": "солнце.png",
                "суд": "суд.png",
                "мир": "мир.png",
            }
            file_name = file_name_corrections.get(file_name_base, f"{file_name_base}.png")
            logger.debug(f"Generated file_name: {file_name}")

            image_path = os.path.join(self.tarot_images_path, file_name)
            if os.path.exists(image_path):
                logger.info(f"Найдено локальное изображение Таро: {image_path}")
            else:
                logger.warning(f"Локальное изображение Таро не найдено: {image_path}")

            # Форматирование вывода по новому шаблону
            formatted_output = f"✨ {card_name}✨\n\n📬 {card_description}"

            logger.info(f"Спарсена карта Таро: {card_title}, file_name={file_name}")
            return card_title, formatted_output, image_path

        except Exception as e:
            logger.error(f"Unexpected error parsing tarot: {e}", exc_info=True)
            return "Карта дня", "✨Карта дня✨\n📬 Не удалось получить описание карты.", None

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = HoroscopeParser()