const { GoogleGenerativeAI } = require("@google/generative-ai");
const User = require("../models/User");

// ✅ Correct Gemini client initialization
const genAI = new GoogleGenerativeAI({
  apiKey: process.env.GEMINI_API_KEY,      // MUST be GEMINI_API_KEY
  baseUrl: "https://generativelanguage.googleapis.com/v1"
});

exports.getChatResponse = async (req, res) => {
  const { message, history } = req.body;

  try {
    const lowerMsg = message.toLowerCase().trim();

    // Appreciation detection
    const appreciationRegex =
      /\b(thanks|thank you|good work|nice|awesome|great|cool|well done|good bot|helpful|ok|okay|good)\b/;

    if (appreciationRegex.test(lowerMsg)) {
      const quickReplies = [
        "👍 Glad you found it useful!",
        "😊 Thank you! I’m always here to guide you on your career journey.",
        "🙌 Happy to help! Let me know what you’d like to explore next.",
        "✨ Great! Ready whenever you want to continue.",
      ];
      const reply =
        quickReplies[Math.floor(Math.random() * quickReplies.length)];

      return res.json({ role: "assistant", content: reply });
    }

    // Special instructions
    const wantsShort =
      /\bshort(en|ly)?\b/.test(lowerMsg) || lowerMsg.includes("short");
    const wantsOneLine =
      /\b(one line|1 line|single line)\b/.test(lowerMsg) ||
      lowerMsg.includes("in one line");

    // Telugu detection
    const isTelugu = /[\u0C00-\u0C7F]/.test(message);

    // Definition request
    const isDefinitionRequest =
      /(gurinchi|cheppu|chepu)/.test(lowerMsg) ||
      /\b(what is|explain|tell me about|definition of)\b/.test(lowerMsg);

    // Fetch user skills
    const user = await User.findById(req.user.id).select("skills");
    const userSkills = user?.skills?.length
      ? user.skills.map((s) => `${s.skillName} (${s.proficiency})`).join(", ")
      : "None";

    // Build system context
    let systemContext;
    if (isTelugu && isDefinitionRequest) {
      systemContext = `The user is asking for a definition/explanation in Telugu.`;
    } else if (isDefinitionRequest) {
      systemContext = `The user is asking for a definition/explanation in English.`;
    } else {
      systemContext = `The user is asking for career advice. They have the following skills: ${userSkills}.`;
    }

    // Build chat history
    const chatHistory = (history || []).map((msg) => ({
      role: msg.role,
      parts: [{ text: msg.content }],
    }));

    // Build final message
    let finalMessage = `${systemContext}\nUser: ${message}`;
    if (wantsShort) {
      finalMessage += "\nAssistant: Please answer briefly in 2-3 sentences.";
    }
    if (wantsOneLine) {
      finalMessage += "\nAssistant: Please answer in exactly one sentence.";
    }
    if (isTelugu && !isDefinitionRequest) {
      finalMessage +=
        "\nAssistant: Respond in Telugu language, focusing on career guidance.";
    }

    // Correct Gemini model usage
    const model = genAI.getGenerativeModel({
      model: "gemini-1.5-flash-latest",
    });

    const result = await model.generateContent({
      contents: [...chatHistory, { role: "user", parts: [{ text: finalMessage }] }],
    });

    const reply =
      result?.response?.candidates?.[0]?.content?.parts?.[0]?.text ||
      "⚠️ Sorry, I couldn’t generate a response.";

    res.json({ role: "assistant", content: reply.trim() });
  } catch (error) {
    console.error("Gemini API Error:", error.message || error);
    res.status(500).json({
      role: "assistant",
      content:
        "⚠️ Sorry, I'm having trouble connecting to my brain right now. Please try again later.",
    });
  }
};
