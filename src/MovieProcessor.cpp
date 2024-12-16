// src/MovieProcessor.cpp

#include "MovieProcessor.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <filesystem>
#include <algorithm>
#include "json.hpp"

using json = nlohmann::json;

// Определение глобальных переменных
std::map<std::string, std::vector<Movie>> genreMap; // Фильмы по жанрам
std::mutex mutexGenre;                              // Мьютекс для защиты данных

// Функция для определения категории оценки
std::string getRatingCategory(double rating) {
    if (rating == 0.0) {
        return "Unrated"; // Специальная категория для рейтинга 0.0
    }

    double min = static_cast<int>(rating / 2.0) * 2.0 + 0.1; // Минимум категории
    double max = min + 1.9;                                  // Максимум категории

    std::ostringstream oss;
    oss << std::fixed << std::setprecision(1) << min << "-" << max;
    return oss.str();
}

// Функция для обработки фильмов одного жанра
void processGenre(const std::string& genre) {
    std::vector<Movie> genreMovies;
    {
        // Защита доступа к общей мапе жанров
        std::lock_guard<std::mutex> lock(mutexGenre);
        auto it = genreMap.find(genre); // Возвращает итератор на элемент карты, у которого ключ совпадает со значением genre
        if (it != genreMap.end()) {
            genreMovies = it->second; // Копирование фильмов одного жанра
        }
    }

    // Сортировка фильмов по категориям оценок
    std::map<std::string, std::vector<std::string>> ratingCategories;
    for (const auto& movie : genreMovies) {
        std::string category = getRatingCategory(movie.rating);
        ratingCategories[category].push_back(movie.title);
    }

    // Создание директории output, если она не существует
    std::string outputDir = "output/";
    try {
        std::filesystem::create_directories(outputDir);
    } catch (const std::filesystem::filesystem_error& e) {
        std::cerr << "Ошибка при создании директории " << outputDir << ": " << e.what() << std::endl;
        return;
    }

    // Запись JSON-файла с распределением по категориям
    std::ofstream jsonFile(outputDir + genre + "_rating_distribution.json");
    if (!jsonFile.is_open()) {
        std::cerr << "Ошибка: Не удалось создать файл для жанра " << genre << std::endl;
        return;
    }

    json jsonOutput;
    for (const auto& [category, titles] : ratingCategories) {
        jsonOutput[category] = titles;
    }
    jsonFile << jsonOutput.dump(4);
    jsonFile.close();
}

// Функция для чтения фильмов из файла
std::vector<Movie> readMovies(const std::string& filename) {
    std::ifstream file(filename);
    if (!file.is_open()) {
        std::cerr << "Ошибка: Не удалось открыть датасет " << filename << std::endl;
        return {};
    }

    std::vector<Movie> movies;
    std::string line;
    int lineNumber = 0;

    while (std::getline(file, line)) {
        lineNumber++;
        if (line.empty()) {
            // Пропуск пустых строк
            continue;
        }

        std::istringstream iss(line);
        Movie movie;

        // Разделяем строку на части по разделителю "|"
        std::string ratingStr;
        if (std::getline(iss, movie.title, '|') &&
            std::getline(iss, movie.genre, '|') &&
            std::getline(iss, ratingStr)) {
            try {
                movie.rating = std::stod(ratingStr); // Преобразование sting в double
                movies.push_back(movie);
            } catch (const std::invalid_argument&) {
                std::cerr << "Некорректный формат рейтинга: \"" << ratingStr << "\". Пропуск строки: \"" << line << "\"." << std::endl;
            }
        } else {
            std::cerr << "Некорректный формат строки на строке " << lineNumber
                      << ": \"" << line << "\". Ожидалось 3 поля, получено "
                      << std::count(line.begin(), line.end(), '|') + 1 << ". Пропуск." << std::endl;
        }
    }

    if (movies.empty()) {
        std::cerr << "Ошибка: Датасет пуст или не удалось распарсить данные." << std::endl;
    }
    return movies;
}

// Функция для параллельного заполнения локальной карты жанров
std::map<std::string, std::vector<Movie>> populateLocalGenreMap(const std::vector<Movie>& movies, size_t start, size_t end) {
    std::map<std::string, std::vector<Movie>> localMap;
    for (size_t i = start; i < end; ++i) {
        const auto& movie = movies[i];
        localMap[movie.genre].push_back(movie);
    }
    return localMap;
}
