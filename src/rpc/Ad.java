package rpc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;

import db.DBConnection;
import db.DBConnectionFactory;
import entity.AdItem;

/**
 * Servlet implementation class Ad
 */
@WebServlet("/Ad")
public class Ad extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private int num = 1;
	private static Comparator<AdItem> Adcomparator = new Comparator<AdItem>(){
 
        @Override
        public int compare(AdItem ad1, AdItem ad2) {
            return -(int)(ad1.getAd_score() * ad1.getBid() - ad2.getAd_score() * ad2.getBid());
        }
    };
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public Ad() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String string = request.getParameter("top");
		if(string!=null && !string.isEmpty()) num = Integer.parseInt(string);
		
		DBConnection conn = DBConnectionFactory.getConnection();
		JSONArray array = new JSONArray();
		
		List<AdItem> items = conn.searchAdItems();
		if (items.size() < num) {
			response.setStatus(404);
			return;
			}
		
		int length = items.size();
		PriorityQueue<AdItem> heap = new PriorityQueue<>(length, Adcomparator);
		heap.addAll(items);
		for(int i=0;i<num;i++) {
			AdItem current = heap.poll();
			AdItem next = heap.peek();
			Double cost = next.getBid()*next.getAd_score()/current.getAd_score()+0.01;
			
			// get and update current budget
			/*
			int advertiser_id = current.getAdvertiser_id();
			double curBudget = conn.getBudget(advertiser_id);
			conn.updateBudget(advertiser_id, curBudget - cost);
			*/
			array.put(current.toJSONObject());
		}
		
		RpcHelper.writeJsonArray(response, array);
		System.out.println("successful");
		conn.close();
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}
