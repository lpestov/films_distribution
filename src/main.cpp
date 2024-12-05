#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <map>
#include <vector>
#include <thread>
#include <mutex>
#include <iomanip>
#include <filesystem>
#include <algorithm>
#include "json.hpp"

using json = nlohmann::json;
using namespace std;

// Структура для хранения данных о фильме
struct Movie {
    string title;
    string genre;
    double rating; // Рейтинг с плавающей запятой
};

// Глобальные переменные
map<string, vector<Movie>> genreMap; // Фильмы по жанрам
mutex mutexGenre;                    // Мьютекс для защиты данных

// Функция для определения категории оценки
string getRatingCategory(double rating) {
    double min = static_cast<int>(rating / 2.0) * 2.0 + 0.1; // Минимум категории
    double max = min + 1.9;                                  // Максимум категории

    ostringstream oss;
    oss << fixed << setprecision(1) << min << "-" << max;
    return oss.str();
}

// Функция для обработки фильмов одного жанра
void processGenre(const string& genre) {
    vector<Movie> genreMovies;
    {
        // Защита доступа к общей карте жанров
        lock_guard<mutex> lock(mutexGenre);
        if (genreMap.find(genre) != genreMap.end()) {
            genreMovies = genreMap[genre];
        }
    }

    // Сортировка фильмов по категориям оценок
    map<string, vector<string>> ratingCategories;
    for (const auto& movie : genreMovies) {
        string category = getRatingCategory(movie.rating);
        ratingCategories[category].push_back(movie.title);
    }

    // Запись JSON-файла с распределением по категориям
    ofstream jsonFile(genre + "_rating_distribution.json");
    if (!jsonFile.is_open()) {
        cerr << "Ошибка: Не удалось создать файл для жанра " << genre << endl;
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
vector<Movie> readMovies(const string& filename) {
    ifstream file(filename);
    if (!file.is_open()) {
        cerr << "Ошибка: Не удалось открыть файл " << filename << endl;
        return {};
    }

    vector<Movie> movies;
    string line;
    int lineNumber = 0;

    while (getline(file, line)) {
        lineNumber++;
        if (line.empty()) {
            // Пропуск пустых строк
            continue;
        }

        istringstream iss(line);
        Movie movie;

        // Разделяем строку на части по разделителю "|"
        string ratingStr;
        if (getline(iss, movie.title, '|') &&
            getline(iss, movie.genre, '|') &&
            getline(iss, ratingStr)) {
            try {
                movie.rating = stod(ratingStr);
                movies.push_back(movie);
            } catch (const invalid_argument&) {
                cerr << "Некорректный формат рейтинга: \"" << ratingStr << "\". Пропуск строки: \"" << line << "\"." << endl;
            }
        } else {
            cerr << "Некорректный формат строки на строке " << lineNumber
                 << ": \"" << line << "\". Ожидалось 3 поля, получено "
                 << count(line.begin(), line.end(), '|') + 1 << ". Пропуск." << endl;
        }
    }

    if (movies.empty()) {
        cerr << "Ошибка: Файл пуст или не удалось распарсить данные." << endl;
    }
    return movies;
}

// Функция для параллельного заполнения локальной карты жанров
map<string, vector<Movie>> populateLocalGenreMap(const vector<Movie>& movies, size_t start, size_t end) {
    map<string, vector<Movie>> localMap;
    for (size_t i = start; i < end; ++i) {
        const auto& movie = movies[i];
        localMap[movie.genre].push_back(movie);
    }
    return localMap;
}

int main() {
    // Вывод текущей рабочей директории
    cout << "Текущая рабочая директория: " << filesystem::current_path() << endl;

    string inputFile = "dataset/movies_cleaned.csv";
    cout << "Путь к входному файлу: " << inputFile << endl;
    // Чтение фильмов из файла
    vector<Movie> movies = readMovies(inputFile);
    if (movies.empty()) {
        cerr << "Нет фильмов для обработки." << endl;
        return -1;
    }

    // Определение количества потоков
    unsigned int numThreads = thread::hardware_concurrency();
    if (numThreads == 0) numThreads = 2; // По умолчанию 2 потока, если невозможно определить

    size_t totalMovies = movies.size();
    size_t chunkSize = totalMovies / numThreads;
    vector<thread> threadsPopulate;
    vector<map<string, vector<Movie>>> localMaps(numThreads);

    // Запуск потоков для заполнения локальных карт жанров
    for (unsigned int i = 0; i < numThreads; ++i) {
        size_t start = i * chunkSize;
        size_t end = (i == numThreads - 1) ? totalMovies : start + chunkSize;
        threadsPopulate.emplace_back([&movies, &localMaps, i, start, end]() {
            localMaps[i] = populateLocalGenreMap(movies, start, end);
        });
    }

    // Ожидание завершения всех потоков
    for (auto& t : threadsPopulate) {
        t.join();
    }

    // Объединение локальных карт в глобальную карту с защитой мьютексом
    {
        lock_guard<mutex> lock(mutexGenre);
        for (const auto& localMap : localMaps) {
            for (const auto& [genre, moviesVec] : localMap) {
                genreMap[genre].insert(genreMap[genre].end(), moviesVec.begin(), moviesVec.end());
            }
        }
    }

    // Создание потоков для обработки каждого жанра
    vector<thread> threadsProcess;
    for (const auto& [genre, _] : genreMap) {
        threadsProcess.emplace_back(processGenre, genre);
    }

    // Ожидание завершения всех потоков обработки жанров
    for (auto& t : threadsProcess) {
        t.join();
    }

    cout << "Фильмы успешно обработаны!" << endl;

    return 0;
}