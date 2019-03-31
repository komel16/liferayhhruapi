import java.io.IOException;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Locale;
import com.google.gson.*;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

public class Main {
    private static final int MAX_VACANCIES = 2000;
    private static final int VACANCY_PER_PAGE = 100;
    private static final String QUERY_HTTP = "https://api.hh.ru/vacancies/";
    private static final String SPECIALIZATION_QUERY = "specialization=1";
    private static final String AREA_QUERY = "area=4";
    private static final String SEARCH_ORDER = "vacancy_search_order=publication_time";


    public static void main(String[] args) {

        CloseableHttpClient httpClient = HttpClients.createDefault();
        try {
            //получаем общее число вакансий
            String query = QUERY_HTTP + "?" + SPECIALIZATION_QUERY + "&" + AREA_QUERY +
                    "&per_page=4&" + SEARCH_ORDER;
            int vacanciesTotalCount = HttpGetPage.getVacanciesTotalCount(httpClient, query);

            //определяем сколько будет страниц с учётом ограничений API
            int maxVacancies = Math.min(vacanciesTotalCount, MAX_VACANCIES); //сколько вакансий можно получить с учётом ограничения API
            int pagesCount = Math.min(Math.floorDiv(maxVacancies, VACANCY_PER_PAGE) + 1, //сколько будет страниц-запросов
                    Math.floorDiv(MAX_VACANCIES, VACANCY_PER_PAGE));
            //System.out.println("pagesCount = " + pagesCount);

            //создаём массив для вакансий
            ArrayList<Vacancy> vacanciesCommonArray = new ArrayList<>(maxVacancies);

            //в цикле постранично запрашиваем данные
            for (int page = 0; page <= pagesCount-1; page++) {
                //System.out.println("page = " + page);

                //получаем массив с вакансиями текущей страницы
                query = QUERY_HTTP + "?" + SPECIALIZATION_QUERY + "&" + AREA_QUERY +
                        "&per_page=" + VACANCY_PER_PAGE + "&page=" + Integer.toString(page) + "&" + SEARCH_ORDER;
                ArrayList<Vacancy> vacanciesPage = HttpGetPage.getVacanciesArray(httpClient, query);

                //добавляем вакансии из массива текущей страницы в общий массив вакансий
                for (int i=0; i <= vacanciesPage.size()-1; i++) {
                    vacanciesCommonArray.add(vacanciesPage.get(i));
                }
            }
            //System.out.println("vacanciesCommonArray.size() = " + vacanciesCommonArray.size());

            JavaToMySQL.VacanciesToDB(vacanciesCommonArray);

        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }

    }
}

class HttpGetPage extends HttpGet {

    static int getVacanciesTotalCount(HttpClient httpClient, String query) throws IOException {
        HttpGet httpGet = new HttpGet(query);
        httpGet.addHeader("User-Agent", "HH-User-Agent");
        CloseableHttpResponse httpResponse = (CloseableHttpResponse) httpClient.execute(httpGet);
        String response = new BasicResponseHandler().handleResponse(httpResponse);

        //преобразуем ответ из строки в JSON
        JsonObject responseJson = new Gson().fromJson(response, JsonObject.class);

        //возращаем общее кол-во найденных вакансий
        return responseJson.getAsJsonPrimitive("found").getAsInt(); //всего вакансий с hh.ru
    }

    static ArrayList<Vacancy> getVacanciesArray (HttpClient httpClient, String query) throws IOException, ParseException {
        ArrayList<Vacancy> vacanciesArray;
        vacanciesArray = new ArrayList<>();

        HttpGet httpGet = new HttpGet(query);
        httpGet.addHeader("User-Agent", "HH-User-Agent");
        CloseableHttpResponse httpResponse = (CloseableHttpResponse) httpClient.execute(httpGet);
        String response = new BasicResponseHandler().handleResponse(httpResponse);

        //преобразуем ответ из строки в JSON
        JsonObject responseJson = new Gson().fromJson(response, JsonObject.class);

        //выделяем json вакансий
        JsonArray vacanciesJson = responseJson.getAsJsonArray("items");

        //проходим по json'у вакансий, каждую вакансию преобразуем в объект
        for (int x = 0; x <= vacanciesJson.size()-1; x++) {
            //выделяем json с вакансией
            JsonObject vacancyJson = vacanciesJson.get(x).getAsJsonObject();
            //создаём объект вакансии
            Vacancy objVacancy = new Vacancy(vacancyJson);

            //кладём вакансию в массив
            vacanciesArray.add(objVacancy);
        }

        return vacanciesArray;
    }

}

class Vacancy {
    private int vacancyID;
    private String vacancyName;
    private String employerName;
    private int salaryFrom;
    private int salaryTo;
    private String salaryCurrency;
    private boolean salaryGross;
    private LocalDate publicDate;

    Vacancy(JsonObject vacancyJson) throws ParseException {
        this.vacancyID = vacancyJson.get("id").getAsInt();
        this.vacancyName = vacancyJson.get("name").getAsString();

        JsonObject employerJson = vacancyJson.getAsJsonObject("employer");
        this.employerName = employerJson.get("name").getAsString();

        if (!vacancyJson.get("salary").isJsonNull()) {
            JsonObject salaryJson = vacancyJson.get("salary").getAsJsonObject();
            if (!salaryJson.get("from").isJsonNull()) {
                this.salaryFrom = salaryJson.get("from").getAsInt();
            }

            if (!salaryJson.get("to").isJsonNull()) {
                this.salaryTo = salaryJson.get("to").getAsInt();
            }

            if (!salaryJson.get("currency").isJsonNull()) {
                this.salaryCurrency = salaryJson.get("currency").getAsString();
            }

            if (!salaryJson.get("gross").isJsonNull()) {
                this.salaryGross = salaryJson.get("gross").getAsBoolean();
            }
        }

        DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ", Locale.ENGLISH);
        java.util.Date utilDate = format.parse(vacancyJson.get("published_at").getAsString());
        Instant instant = utilDate.toInstant();
        ZoneId zoneId = ZoneId.of("Europe/Moscow");
        ZonedDateTime zdt = ZonedDateTime.ofInstant (instant , zoneId);
        this.publicDate = zdt.toLocalDate();
    }

    int getVacancyID() {
        return vacancyID;
    }

    String getVacancyName() {
        return vacancyName;
    }

    String getEmployerName() {
        return employerName;
    }

    int getSalaryFrom() {
        return salaryFrom;
    }

    int getSalaryTo() {
        return salaryTo;
    }

    String getSalaryCurrency() {
        return salaryCurrency;
    }

    boolean isSalaryGross() {
        return salaryGross;
    }

    LocalDate getPublicDate() {
        return publicDate;
    }
}


class JavaToMySQL {

    private static final String jdbcURL = "jdbc:mysql://localhost:3306/neighbourhood_portal";
    private static final String USER = "root";
    private static final String PASSWORD = "admin";

    static void VacanciesToDB (ArrayList<Vacancy> vacanciesArray) {
        try {
            Connection connection = DriverManager.getConnection(jdbcURL, USER, PASSWORD);

            //очищаем таблицу в базе данных
            Statement simpleQuery = connection.createStatement();
            simpleQuery.execute("TRUNCATE TABLE neighbourhood_portal.vacancies");

            //формируем запрос-пачку
            String batchQuery = "INSERT INTO neighbourhood_portal.vacancies " +
                    "(id_vacancy, " +
                    "vacancy_name, " +
                    "employer_name, " +
                    "salary_from, " +
                    "salary_to, " +
                    "salary_currency, " +
                    "salary_gross, " +
                    "public_date) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement prStmt = connection.prepareStatement(batchQuery);
            for (Vacancy vacancy : vacanciesArray) {
                prStmt.setInt(1, vacancy.getVacancyID());
                prStmt.setString(2, vacancy.getVacancyName());
                prStmt.setString(3, vacancy.getEmployerName());
                prStmt.setInt(4, vacancy.getSalaryFrom());
                prStmt.setInt(5, vacancy.getSalaryTo());
                prStmt.setString(6, vacancy.getSalaryCurrency());
                prStmt.setBoolean(7, vacancy.isSalaryGross());
                prStmt.setDate(8, java.sql.Date.valueOf(vacancy.getPublicDate()));
                prStmt.addBatch();
            }

            prStmt.executeBatch();
            prStmt.close();
            connection.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}