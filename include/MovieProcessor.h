// include/MovieProcessor.h

#ifndef MOVIEPROCESSOR_H
#define MOVIEPROCESSOR_H

#include <string>
#include <vector>
#include <map>
#include <mutex>

// Структура для хранения данных о фильме
struct Movie {
    std::string title;
    std::string genre;
    double rating;
};

// Глобальные переменные
extern std::map<std::string, std::vector<Movie>> genreMap; // Фильмы по жанрам
extern std::mutex mutexGenre;                              // Предотвращаем доступ к genreMap из разных потоков

// Функция для определения категории оценки
std::string getRatingCategory(double rating);

// Функция для обработки фильмов одного жанра
void processGenre(const std::string& genre);

// Функция для чтения фильмов из файла
std::vector<Movie> readMovies(const std::string& filename);

// Функция для параллельного заполнения локальной карты жанров
std::map<std::string, std::vector<Movie>> populateLocalGenreMap(const std::vector<Movie>& movies, size_t start, size_t end);

#endif // MOVIEPROCESSOR_H
