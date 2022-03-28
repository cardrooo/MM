

SELECT studentName
FROM student
WHERE studentID in (		
	SELECT studentID
	FROM examResult
	WHERE subject = 'Maths' 
		AND marks = (
			SELECT MAX(marks)
			FROM examResult
			WHERE subject = 'Maths'
		)
	)



SELECT studentName
FROM student
WHERE studentID in (		
	SELECT studentID
	FROM examResult
	WHERE subject = 'English' 
		AND marks = (
			SELECT MIN(marks)
			FROM examResult
			WHERE subject = 'English'
		)
	)



SELECT 
	s.studentName, 
	AVG(marks) as AvgMarks
FROM student s
INNER JOIN examResult e
	ON s.studentID = e.studentID
GROUP BY s.studentName
ORDER BY AVG(marks) DESC


SELECT studentName
FROM student
WHERE LEFT(studentName, 1) = 'A'
--where UPPER(LEFT(studentName, 1)) = 'A'



SELECT COUNT(e.studentID) AS SmartyPants
FROM examResult e
GROUP BY e.studentID
HAVING SUM(CASE WHEN marks >= 85 THEN 1 ELSE 0 END) >= 2


