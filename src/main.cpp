// src/main.cpp

#include <iostream>
#include <vector>
#include <thread>
#include <map>
#include "MovieProcessor.h"

int main() {
    std::string inputFile = "dataset/movies_cleaned.csv";
    std::cout << "Путь к входному файлу: " << inputFile << std::endl;

    // Чтение фильмов из файла
    std::vector<Movie> movies = readMovies(inputFile);
    if (movies.empty() ) {
        std::cerr << "Нет фильмов для обработки." << std::endl;
        return -1;
    }

    // Определение количества потоков
    unsigned int numThreads = std::thread::hardware_concurrency();
    if (numThreads == 0) numThreads = 2; // По умолчанию 2 потока, если невозможно определить

    size_t totalMovies = movies.size();
    size_t chunkSize = totalMovies / numThreads;
    std::vector<std::thread> threadsPopulate;
    std::vector<std::map<std::string, std::vector<Movie>>> localMaps(numThreads);

    // Запуск потоков для заполнения локальных карт жанров
    for (unsigned int i = 0; i < numThreads; ++i) {
        size_t start = i * chunkSize;
        size_t end = (i == numThreads - 1) ? totalMovies : start + chunkSize;
        threadsPopulate.emplace_back([&movies, &localMaps, i, start, end]() {
            localMaps[i] = populateLocalGenreMap(movies, start, end);
        });
    }

    // Ожидание завершения всех потоков, блокируем главный поток пока все потоки не завершатся
    for (auto& t : threadsPopulate) {
        t.join();
    }

    // Объединение локальных карт в глобальную карту с защитой мьютексом
    {
        std::lock_guard<std::mutex> lock(mutexGenre);
        for (const auto& localMap : localMaps) {
            for (const auto& [genre, moviesVec] : localMap) {
                genreMap[genre].insert(genreMap[genre].end(), moviesVec.begin(), moviesVec.end());
            }
        }
    }

    // Создание потоков для обработки каждого жанра
    std::vector<std::thread> threadsProcess;
    for (const auto& [genre, _] : genreMap) {
        threadsProcess.emplace_back(processGenre, genre);
    }

    // Ожидание завершения всех потоков обработки жанров
    for (auto& t : threadsProcess) {
        t.join();
    }

    std::cout << "Фильмы успешно обработаны!" << std::endl;

    return 0;
}
