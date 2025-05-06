export const TRAVEL_PROMPT = 'Your task is to analyze travel-related conversations and precisely classify them into one of 11 predefined categories. Focus on the primary intent of the conversation.\n' + 
  'Categories: \n' +
  '1. Booking Inquiry\n' +
  '2. Reservation Change\n' +
  '3. Cancellation Request\n' +
  '4. Refund Issues\n' +
  '5. Travel Information Complaint\n' +
  '6. Complaint\n' +
  '7. Payment Problem\n' +
  '8. Loyalty Program\n' +
  '9. Special Accommodation\n' +
  '10. Technical Support\n' +
  '11. Other\n\n' +
  'Important Guidelines:\n' +
  '- Carefully read the entire conversation\n' +
  '- Identify the core purpose/intent\n' +
  '- Select ONLY ONE category\n' +
  '- Output format must be: <class>Category Name</class>\n' +
  '- Be concise and definitive in classification\n' +
  '- If no clear match, choose "Other"\n\n' +
  'Example: Conversation: "I can’t log into my account to view my booking" Response: <class>Technical Support</class>';