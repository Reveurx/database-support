-- Q1 - "Репутационные пары".
WITH 
-- Все вопросы с тегом postgresql
postgresql_questions AS (
    SELECT p.Id, p.CreationDate, p.Tags, p.AcceptedAnswerId, regexp_split_to_table(NULLIF(trim(both '|' FROM p.Tags), ''), '\|') AS single_tag
    FROM Posts p
    WHERE 
        p.PostTypeId = 1 -- Вопросы
        AND p.Tags LIKE '%|postgresql|%'
        AND p.AcceptedAnswerId IS NOT NULL),

-- Топ-20 пар тегов
top_tag_pairs AS (
    SELECT 'postgresql' AS tag1, single_tag AS tag2, COUNT(*) AS pair_count
    FROM postgresql_questions
    WHERE 
        single_tag NOT IN ('postgresql', '', ' ', '  ')
        AND single_tag IS NOT NULL
    GROUP BY single_tag
    ORDER BY pair_count DESC
    LIMIT 50),

-- Валидные ответы с расчетом времени
valid_answers AS (
    SELECT 
        a.Id AS answer_id,
        a.CreationDate AS answer_date,
        a.OwnerUserId,
        u.Reputation,
        q.Id AS question_id,
        q.CreationDate AS question_date,
        q.Tags AS question_tags,
        tp.tag1,
        tp.tag2,
        GREATEST(EXTRACT(EPOCH FROM (a.CreationDate - q.CreationDate))/3600, 0) AS hours_to_answer
    FROM top_tag_pairs tp
    JOIN postgresql_questions q ON q.Tags LIKE '%|' || tp.tag1 || '|%' 
                              AND q.Tags LIKE '%|' || tp.tag2 || '|%'
    JOIN Posts a ON a.Id = q.AcceptedAnswerId
               AND a.PostTypeId = 2 -- Ответы
               -- AND a.CreationDate >= q.CreationDate -- Мин. время может принимать отрицательные значения, но его стоит сохранить для отсутствия смещения среднего и компенсации положительного сдвига в ответах
    JOIN Users u ON a.OwnerUserId = u.Id
    WHERE a.OwnerUserId IS NOT NULL),

-- Агрегированные данные по парам тегов
tag_pair_stats AS (
    SELECT
        tag1, tag2,
        COUNT(*) AS question_count,
        AVG(hours_to_answer) AS avg_hours,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY hours_to_answer) AS median_hours,
        AVG(Reputation) AS avg_reputation,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Reputation) AS median_reputation,
        -- Данные для расчета корреляции (-1; 1)
        COVAR_POP(hours_to_answer, Reputation) / (STDDEV_POP(hours_to_answer) * STDDEV_POP(Reputation)) AS pearson_corr,
        COUNT(DISTINCT OwnerUserId) AS unique_responders
    FROM valid_answers
    GROUP BY tag1, tag2
    HAVING COUNT(*) >= 5
)

-- Результат с форматированием
SELECT
    tag1, tag2, question_count,
    ROUND(avg_hours::numeric, 2) AS avg_hours_to_answer,
    ROUND(median_hours::numeric, 2) AS median_hours_to_answer,
    ROUND(avg_reputation::numeric, 2) AS avg_responder_reputation,
    ROUND(median_reputation::numeric, 2) AS median_responder_reputation,
    ROUND(pearson_corr::numeric, 4) AS time_reputation_correlation
FROM tag_pair_stats
ORDER BY question_count DESC, avg_hours ASC;

-- Q2 - "Успешные шутники".
WITH 
-- Находим вопросы с тегом postgresql
postgresql_questions AS (
    SELECT p.Id, p.AcceptedAnswerId
    FROM Posts p
    WHERE p.PostTypeId = 1 -- Вопросы
        AND p.Tags LIKE '%|postgresql|%'
        AND p.AcceptedAnswerId IS NOT NULL),

-- Находим принятые ответы с отрицательным рейтингом
low_score_accepted_answers AS (
    SELECT 
        a.Id AS answer_id,
        a.Score,
        a.Body,
        a.CreationDate,
        u.Id AS user_id,
        u.DisplayName AS user_name,
        u.Reputation,
        q.Id AS question_id
    FROM postgresql_questions q
    JOIN Posts a ON a.Id = q.AcceptedAnswerId
               AND a.PostTypeId = 2 -- Ответы
    JOIN Users u ON a.OwnerUserId = u.Id)

-- Финальный результат
SELECT 
    answer_id AS "ID ответа",
    Score AS "Рейтинг",
    user_name AS "Автор ответа",
    Reputation AS "Репутация автора",
    CreationDate AS "Дата ответа",
    question_id AS "ID вопроса",
    -- Обрезаем тело ответа
    CASE 
        WHEN length(Body) > 100 THEN substring(Body, 1, 100) || '...'
        ELSE Body
    END AS "Текст ответа (начало)"
FROM low_score_accepted_answers
ORDER BY Score ASC -- Сначала худшие по рейтингу
LIMIT 50;
